//! The merged candidate: Elixir terms -> Arrow IPC directly, no protobuf hop
//! in between. Reuses the same type-inference/widening approach proven in
//! `dynamic.rs`, but reads Elixir's own dynamically-typed `Term` directly
//! instead of a protobuf `Value` - Rustler's term type checks act as the
//! tagged union, so no intermediate wire format is needed at all for this
//! step. Also folds in what `EventUtils.wrap_body_fields/1` and
//! `normalize_df_struct_fields/1` do today in Elixir (nested-map-as-
//! length-1-list wrapping, cross-row key union) - both now run once, in
//! Rust, as part of the same pass that builds the schema.
//!
//! Takes the *raw* `LogEvent.body` shape directly: `timestamp` as a plain
//! integer (no `DateTime` round-trip needed - that conversion in
//! `log_event_to_df_struct/1` existed only because Jason needed to
//! serialize it as text), `event_message`, and everything else as
//! unconstrained metadata.

use crate::{timestamp_field, write_ipc};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, ListArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use rustler::types::map::MapIterator;
use rustler::{Binary, Env, Error, NifResult, OwnedBinary, Term};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
enum FieldType {
    Utf8,
    Float64,
    Boolean,
    StringList,
    Struct(Vec<(String, FieldType)>),
}

fn infer_term_type(term: Term) -> FieldType {
    if term.decode::<bool>().is_ok() {
        FieldType::Boolean
    } else if term.is_binary() {
        FieldType::Utf8
    } else if term.is_number() {
        FieldType::Float64
    } else if term.is_map() {
        FieldType::Struct(infer_struct_fields(term))
    } else if term.is_list() {
        FieldType::StringList
    } else {
        FieldType::Utf8
    }
}

fn infer_struct_fields(map: Term) -> Vec<(String, FieldType)> {
    let mut acc: BTreeMap<String, FieldType> = BTreeMap::new();
    if let Some(iter) = MapIterator::new(map) {
        for (k, v) in iter {
            if let Ok(key) = k.decode::<String>() {
                let t = infer_term_type(v);
                acc.entry(key)
                    .and_modify(|existing| *existing = widen(existing.clone(), t.clone()))
                    .or_insert(t);
            }
        }
    }
    acc.into_iter().collect()
}

/// Conflicting types for the same key -> widen to `Utf8`, same explicit
/// policy as `dynamic.rs`. Two structs widen by recursively unioning their
/// fields rather than collapsing straight to a string, since a struct
/// conflicting with a struct is just "different optional sub-fields," not a
/// real type conflict.
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
        (a, b) if a == b => a,
        _ => FieldType::Utf8,
    }
}

fn field_data_type(t: &FieldType) -> DataType {
    match t {
        FieldType::Utf8 => DataType::Utf8,
        FieldType::Float64 => DataType::Float64,
        FieldType::Boolean => DataType::Boolean,
        FieldType::StringList => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
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

/// Builds one Arrow column for `name` across every row, given the union type
/// already decided for it. `rows` holds each row's metadata as already-
/// extracted `(key, Term)` pairs so nested terms stay valid for the whole
/// build (Rustler terms are tied to the NIF call's `Env`).
fn build_array(rows: &[HashMap<String, Term>], name: &str, t: &FieldType) -> NifResult<ArrayRef> {
    match t {
        FieldType::Utf8 => {
            let values: Vec<Option<String>> =
                rows.iter().map(|r| r.get(name).map(|t| term_as_string(*t))).collect();
            Ok(Arc::new(StringArray::from(values)))
        }
        FieldType::Float64 => {
            let values: Vec<Option<f64>> = rows
                .iter()
                .map(|r| r.get(name).and_then(|t| t.decode::<f64>().ok()))
                .collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        FieldType::Boolean => {
            let values: Vec<Option<bool>> = rows
                .iter()
                .map(|r| r.get(name).and_then(|t| t.decode::<bool>().ok()))
                .collect();
            Ok(Arc::new(BooleanArray::from(values)))
        }
        FieldType::StringList => {
            let mut flat: Vec<String> = Vec::new();
            let mut lengths: Vec<usize> = Vec::new();
            let mut present: Vec<bool> = Vec::new();
            for r in rows {
                match r.get(name).and_then(|t| t.decode::<Vec<String>>().ok()) {
                    Some(items) => {
                        lengths.push(items.len());
                        present.push(true);
                        flat.extend(items);
                    }
                    None => {
                        lengths.push(0);
                        present.push(false);
                    }
                }
            }
            Ok(Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Utf8, true)),
                OffsetBuffer::from_lengths(lengths),
                Arc::new(StringArray::from(flat)),
                Some(NullBuffer::from(present)),
            )))
        }
        FieldType::Struct(fields) => {
            // Per row: the nested map's own (key, Term) pairs if present and
            // actually a map, else nothing - same shape as the top-level
            // `rows` input, one level down.
            let mut inner_rows: Vec<HashMap<String, Term>> = Vec::with_capacity(rows.len());
            let mut present: Vec<bool> = Vec::with_capacity(rows.len());
            for r in rows {
                match r.get(name) {
                    Some(t) if t.is_map() => {
                        inner_rows.push(extract_fields(*t));
                        present.push(true);
                    }
                    _ => {
                        inner_rows.push(HashMap::new());
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
                child_arrays.push(build_array(&inner_rows, field_name, field_type)?);
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

fn extract_fields(map: Term) -> HashMap<String, Term> {
    let mut out = HashMap::new();
    if let Some(iter) = MapIterator::new(map) {
        for (k, v) in iter {
            if let Ok(key) = k.decode::<String>() {
                out.insert(key, v);
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
fn encode_rows_to_arrow_ipc<'a>(env: Env<'a>, rows: Vec<Term<'a>>) -> NifResult<(Binary<'a>, Vec<Binary<'a>>)> {
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
    let encoded = write_ipc(&schema, &batch).map_err(|e| Error::Term(Box::new(e)))?;

    let schema_binary = to_binary(env, &encoded.schema);
    let batch_binaries = encoded.batches.iter().map(|b| to_binary(env, b)).collect();
    Ok((schema_binary, batch_binaries))
}
