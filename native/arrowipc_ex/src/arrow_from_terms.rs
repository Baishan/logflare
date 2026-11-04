//! Encodes rows directly from Elixir terms into Arrow IPC bytes, with no
//! JSON text in between. `get_ipc_bytes/2` (in `lib.rs`) infers a schema by
//! serializing to JSON text and parsing it twice (`infer_json_schema`, then
//! `ReaderBuilder`); this module does the same job by reading Elixir's
//! already-typed `Term`s directly - Rustler's term type checks act as the
//! tagged union, so no intermediate wire format is needed.
//!
//! Takes the *raw* `LogEvent.body` shape: `timestamp` as a plain integer (no
//! `DateTime` round-trip needed - that conversion in
//! `EventUtils.log_event_to_df_struct/1` exists only so `Jason` can
//! serialize it as text), `event_message`, and everything else as
//! unconstrained metadata. Folds in what `EventUtils.wrap_body_fields/1` and
//! `normalize_df_struct_fields/1` do today in Elixir (nested-map-as-
//! length-1-list wrapping, cross-row key union) - both run once here, in
//! Rust, as part of the same pass that builds the schema.
//!
//! Scope cut, documented rather than silent: a list element that is itself
//! a struct or another list falls back to a stringified `Utf8` element, same
//! as a genuine type conflict (see `widen/2`). Non-UTF8 binary metadata is
//! also not faithfully preserved (falls back to a debug-style string) -
//! known gap, not yet resolved either here or in the JSON path, which
//! raises on the same input.

use crate::{build_write_options, timestamp_field, write_ipc, EncodedBatch};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use rustler::types::map::MapIterator;
use rustler::{Atom, Binary, Env, Error, NifResult, OwnedBinary, Term};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
enum FieldType {
    Utf8,
    Int64,
    Float64,
    Boolean,
    List(Box<FieldType>),
    Struct(Vec<(String, FieldType)>),
}

/// `nil` is Elixir's explicit null, distinct from a key simply being absent
/// - but it must be treated identically to "absent" here. Without this
/// check, a `nil` value gets classified as `Utf8` (it matches none of the
/// other term-shape checks), which both loses the nullness and - via
/// `widen` - corrupts the *whole column* to a string the moment any row has
/// an explicit `nil` for an otherwise-numeric field.
fn is_nil(term: Term) -> bool {
    term.decode::<rustler::Atom>()
        .map(|a| a == rustler::types::atom::nil())
        .unwrap_or(false)
}

fn infer_term_type(term: Term) -> FieldType {
    if term.decode::<bool>().is_ok() {
        FieldType::Boolean
    } else if term.is_binary() {
        FieldType::Utf8
    } else if term.decode::<i64>().is_ok() {
        FieldType::Int64
    } else if term.is_number() {
        FieldType::Float64
    } else if term.is_map() {
        FieldType::Struct(infer_struct_fields(term))
    } else if term.is_list() {
        let elem_type = term
            .decode::<Vec<Term>>()
            .unwrap_or_default()
            .into_iter()
            .filter(|e| !is_nil(*e))
            .map(infer_term_type)
            .reduce(widen)
            .unwrap_or(FieldType::Utf8);
        FieldType::List(Box::new(elem_type))
    } else {
        FieldType::Utf8
    }
}

fn infer_struct_fields(map: Term) -> Vec<(String, FieldType)> {
    let mut acc: BTreeMap<String, FieldType> = BTreeMap::new();
    if let Some(iter) = MapIterator::new(map) {
        for (k, v) in iter {
            if let Ok(key) = k.decode::<String>() {
                if is_nil(v) {
                    continue;
                }
                let t = infer_term_type(v);
                acc.entry(key)
                    .and_modify(|existing| *existing = widen(existing.clone(), t.clone()))
                    .or_insert(t);
            }
        }
    }
    acc.into_iter().collect()
}

/// Conflicting types for the same key -> widen to `Utf8`. Two structs widen
/// by recursively unioning their fields rather than collapsing to a string,
/// since a struct conflicting with a struct is just "different optional
/// sub-fields," not a real type conflict. A list element that's a struct or
/// another list is deliberately not given its own recursive widening - it
/// collapses to `Utf8` like any other conflict, per the module-level scope
/// cut.
fn widen(a: FieldType, b: FieldType) -> FieldType {
    match (a, b) {
        (FieldType::Struct(af), FieldType::Struct(bf)) => {
            let mut map: BTreeMap<String, FieldType> = af.into_iter().collect();
            for (k, t) in bf {
                map.entry(k)
                    .and_modify(|existing| *existing = widen(existing.clone(), t.clone()))
                    .or_insert(t);
            }
            FieldType::Struct(map.into_iter().collect())
        }
        (FieldType::List(ae), FieldType::List(be)) => FieldType::List(Box::new(widen(*ae, *be))),
        // Int64 + Float64 is a real, common case (e.g. `1` on one row, `1.5`
        // on another) - numeric widening, not a type conflict.
        (FieldType::Int64, FieldType::Float64) | (FieldType::Float64, FieldType::Int64) => {
            FieldType::Float64
        }
        (a, b) if a == b => a,
        _ => FieldType::Utf8,
    }
}

fn field_data_type(t: &FieldType) -> DataType {
    match t {
        FieldType::Utf8 => DataType::Utf8,
        FieldType::Int64 => DataType::Int64,
        FieldType::Float64 => DataType::Float64,
        FieldType::Boolean => DataType::Boolean,
        FieldType::List(elem) => DataType::List(Arc::new(Field::new("item", field_data_type(elem), true))),
        FieldType::Struct(fields) => {
            let struct_fields: Fields = fields
                .iter()
                .map(|(name, t)| Field::new(name, field_data_type(t), true))
                .collect();
            // Nested map -> BigQuery REPEATED RECORD, i.e. wrapped in a list,
            // mirroring `EventUtils.wrap_body_fields/1`.
            DataType::List(Arc::new(Field::new("item", DataType::Struct(struct_fields), true)))
        }
    }
}

fn term_as_string(term: Term) -> String {
    if let Ok(s) = term.decode::<String>() {
        s
    } else if let Ok(n) = term.decode::<f64>() {
        n.to_string()
    } else if let Ok(b) = term.decode::<bool>() {
        b.to_string()
    } else {
        format!("{term:?}")
    }
}

/// Builds one Arrow array from a flat, already-aligned list of optional
/// terms - either a top-level/nested-struct column (one term per row) or a
/// list column's flattened elements (one term per element). Recursive: a
/// `Struct` or `List` field type recurses into this same function one level
/// down.
fn build_scalar_array(values: &[Option<Term>], t: &FieldType) -> NifResult<ArrayRef> {
    match t {
        FieldType::Utf8 => {
            let out: Vec<Option<String>> = values.iter().map(|v| v.map(term_as_string)).collect();
            Ok(Arc::new(StringArray::from(out)))
        }
        FieldType::Int64 => {
            let out: Vec<Option<i64>> =
                values.iter().map(|v| v.and_then(|t| t.decode::<i64>().ok())).collect();
            Ok(Arc::new(Int64Array::from(out)))
        }
        FieldType::Float64 => {
            // A column can widen to Float64 from a mix of Int64 and Float64
            // values, so a term that only decodes as an integer must still
            // be accepted here, not read as absent.
            let out: Vec<Option<f64>> = values
                .iter()
                .map(|v| {
                    v.and_then(|t| {
                        t.decode::<f64>().ok().or_else(|| t.decode::<i64>().ok().map(|i| i as f64))
                    })
                })
                .collect();
            Ok(Arc::new(Float64Array::from(out)))
        }
        FieldType::Boolean => {
            let out: Vec<Option<bool>> =
                values.iter().map(|v| v.and_then(|t| t.decode::<bool>().ok())).collect();
            Ok(Arc::new(BooleanArray::from(out)))
        }
        FieldType::List(elem_type) => {
            let mut flat: Vec<Option<Term>> = Vec::new();
            let mut lengths: Vec<usize> = Vec::new();
            let mut present: Vec<bool> = Vec::new();
            for v in values {
                match v.and_then(|t| if t.is_list() { t.decode::<Vec<Term>>().ok() } else { None }) {
                    Some(items) => {
                        lengths.push(items.len());
                        present.push(true);
                        flat.extend(items.into_iter().map(|e| if is_nil(e) { None } else { Some(e) }));
                    }
                    None => {
                        lengths.push(0);
                        present.push(false);
                    }
                }
            }
            let child = build_scalar_array(&flat, elem_type)?;
            Ok(Arc::new(ListArray::new(
                Arc::new(Field::new("item", field_data_type(elem_type), true)),
                OffsetBuffer::from_lengths(lengths),
                child,
                Some(NullBuffer::from(present)),
            )))
        }
        FieldType::Struct(fields) => {
            let mut inner_values: Vec<HashMap<String, Term>> = Vec::with_capacity(values.len());
            let mut present: Vec<bool> = Vec::with_capacity(values.len());
            for v in values {
                match v {
                    Some(t) if t.is_map() => {
                        inner_values.push(extract_fields(*t));
                        present.push(true);
                    }
                    _ => {
                        inner_values.push(HashMap::new());
                        present.push(false);
                    }
                }
            }

            let struct_fields: Fields = fields
                .iter()
                .map(|(name, t)| Field::new(name, field_data_type(t), true))
                .collect();
            let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
            for (field_name, field_type) in fields {
                let field_values: Vec<Option<Term>> =
                    inner_values.iter().map(|m| m.get(field_name.as_str()).copied()).collect();
                child_arrays.push(build_scalar_array(&field_values, field_type)?);
            }
            let struct_array = StructArray::new(struct_fields.clone(), child_arrays, None);

            Ok(Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Struct(struct_fields), true)),
                OffsetBuffer::from_lengths(present.iter().map(|p| if *p { 1 } else { 0 })),
                Arc::new(struct_array),
                Some(NullBuffer::from(present)),
            )))
        }
    }
}

fn build_array(rows: &[HashMap<String, Term>], name: &str, t: &FieldType) -> NifResult<ArrayRef> {
    let values: Vec<Option<Term>> = rows.iter().map(|r| r.get(name).copied()).collect();
    build_scalar_array(&values, t)
}

fn extract_fields(map: Term) -> HashMap<String, Term> {
    let mut out = HashMap::new();
    if let Some(iter) = MapIterator::new(map) {
        for (k, v) in iter {
            if let Ok(key) = k.decode::<String>() {
                if !is_nil(v) {
                    out.insert(key, v);
                }
            }
        }
    }
    out
}

fn to_binary<'a>(env: Env<'a>, bytes: &[u8]) -> Binary<'a> {
    let mut binary = OwnedBinary::new(bytes.len()).unwrap();
    binary.as_mut_slice().copy_from_slice(bytes);
    binary.release(env)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn get_ipc_bytes_from_rows<'a>(
    env: Env<'a>,
    rows: Vec<Term<'a>>,
    compression: Atom,
) -> NifResult<(Binary<'a>, Vec<Binary<'a>>)> {
    let compression_type = crate::atom_to_compression(compression)?;
    let write_options = build_write_options(compression_type);

    let mut timestamps: Vec<i64> = Vec::with_capacity(rows.len());
    let mut event_messages: Vec<String> = Vec::with_capacity(rows.len());
    let mut metadatas: Vec<HashMap<String, Term<'a>>> = Vec::with_capacity(rows.len());

    for row in &rows {
        let mut fields = extract_fields(*row);

        let timestamp_term = fields.remove("timestamp").ok_or(Error::BadArg)?;
        let event_message_term = fields.remove("event_message").ok_or(Error::BadArg)?;

        timestamps.push(timestamp_term.decode()?);
        event_messages.push(event_message_term.decode()?);
        metadatas.push(fields);
    }

    let field_schema = {
        let mut acc: BTreeMap<String, FieldType> = BTreeMap::new();
        for metadata in &metadatas {
            for (k, v) in metadata {
                let t = infer_term_type(*v);
                acc.entry(k.clone())
                    .and_modify(|existing| *existing = widen(existing.clone(), t.clone()))
                    .or_insert(t);
            }
        }
        acc.into_iter().collect::<Vec<_>>()
    };

    let mut schema_fields = vec![timestamp_field(), Field::new("event_message", DataType::Utf8, true)];
    for (name, t) in &field_schema {
        schema_fields.push(Field::new(name, field_data_type(t), true));
    }
    let schema = Schema::new(schema_fields);

    let timestamp_array = TimestampMicrosecondArray::from(timestamps).with_timezone("+00");
    let event_message_array = StringArray::from(event_messages);

    let mut arrays: Vec<ArrayRef> = vec![Arc::new(timestamp_array), Arc::new(event_message_array)];
    for (name, t) in &field_schema {
        arrays.push(build_array(&metadatas, name, t)?);
    }

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| Error::Term(Box::new(e.to_string())))?;
    let encoded: EncodedBatch =
        write_ipc(&schema, &batch, &write_options).map_err(|e| Error::Term(Box::new(e)))?;

    let schema_binary = to_binary(env, &encoded.schema);
    let batch_binaries = encoded.batches.iter().map(|b| to_binary(env, b)).collect();
    Ok((schema_binary, batch_binaries))
}
