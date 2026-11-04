//! De-risks the scope cut in `lib.rs`: real log metadata has keys that vary
//! per row, and types that occasionally conflict for the same key (e.g. a
//! field that's usually a number but is a string on some rows). This module
//! tests whether the "protobuf in, Arrow out, no text parsing" approach
//! still works — and is still fast — once we stop assuming a uniform schema.
//!
//! The answer still requires *some* inference (you can't build one Arrow
//! column per field without knowing each field's type across the whole
//! batch) — the claim was never "no inference," it was "no text parsing."
//! This module infers types from already-typed protobuf `Value`s instead of
//! from JSON text, and widens conflicting types to a string column rather
//! than erroring, which is a policy choice, not an accident — see `widen`.

use crate::pb;
use crate::{timestamp_field, write_ipc, EncodedBatch};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, ListArray, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::{DateTime, SecondsFormat, Utc};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FieldType {
    Utf8,
    Float64,
    Boolean,
    StringList,
}

fn value_type(v: &pb::Value) -> FieldType {
    match &v.kind {
        Some(pb::value::Kind::StringValue(_)) | None => FieldType::Utf8,
        Some(pb::value::Kind::NumberValue(_)) => FieldType::Float64,
        Some(pb::value::Kind::BoolValue(_)) => FieldType::Boolean,
        Some(pb::value::Kind::ListValue(_)) => FieldType::StringList,
    }
}

/// Two different types seen for the same key across the batch: fall back to
/// a string column (stringifying every value for that field) rather than
/// erroring or silently dropping data. Same spirit as how loosely-typed
/// systems usually resolve this, made explicit instead of inherited from
/// whatever `infer_json_schema` happens to decide.
fn widen(a: FieldType, b: FieldType) -> FieldType {
    if a == b {
        a
    } else {
        FieldType::Utf8
    }
}

/// Union schema across the whole batch: every key seen in any row's
/// metadata, each resolved to one type (widened to `Utf8` on conflict).
/// BTreeMap keeps field order deterministic for reproducible comparisons.
fn infer_schema(rows: &[pb::LogRowDynamic]) -> Vec<(String, FieldType)> {
    let mut acc: BTreeMap<String, FieldType> = BTreeMap::new();
    for row in rows {
        for (k, v) in &row.metadata {
            let t = value_type(v);
            acc.entry(k.clone())
                .and_modify(|existing| *existing = widen(*existing, t))
                .or_insert(t);
        }
    }
    acc.into_iter().collect()
}

fn value_as_string(v: &pb::Value) -> String {
    match &v.kind {
        Some(pb::value::Kind::StringValue(s)) => s.clone(),
        Some(pb::value::Kind::NumberValue(n)) => n.to_string(),
        Some(pb::value::Kind::BoolValue(b)) => b.to_string(),
        Some(pb::value::Kind::ListValue(l)) => format!("{:?}", l.values),
        None => String::new(),
    }
}

fn field_data_type(t: FieldType) -> DataType {
    match t {
        FieldType::Utf8 => DataType::Utf8,
        FieldType::Float64 => DataType::Float64,
        FieldType::Boolean => DataType::Boolean,
        FieldType::StringList => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
    }
}

fn build_field_array(rows: &[pb::LogRowDynamic], name: &str, t: FieldType) -> ArrayRef {
    match t {
        FieldType::Utf8 => {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|r| r.metadata.get(name).map(value_as_string))
                .collect();
            Arc::new(StringArray::from(values))
        }
        FieldType::Float64 => {
            let values: Vec<Option<f64>> = rows
                .iter()
                .map(|r| match r.metadata.get(name).and_then(|v| v.kind.as_ref()) {
                    Some(pb::value::Kind::NumberValue(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            Arc::new(Float64Array::from(values))
        }
        FieldType::Boolean => {
            let values: Vec<Option<bool>> = rows
                .iter()
                .map(|r| match r.metadata.get(name).and_then(|v| v.kind.as_ref()) {
                    Some(pb::value::Kind::BoolValue(b)) => Some(*b),
                    _ => None,
                })
                .collect();
            Arc::new(BooleanArray::from(values))
        }
        FieldType::StringList => {
            let mut flat: Vec<&str> = Vec::new();
            let mut lengths: Vec<usize> = Vec::new();
            let mut present: Vec<bool> = Vec::new();
            for r in rows {
                match r.metadata.get(name).and_then(|v| v.kind.as_ref()) {
                    Some(pb::value::Kind::ListValue(l)) => {
                        lengths.push(l.values.len());
                        present.push(true);
                        flat.extend(l.values.iter().map(String::as_str));
                    }
                    _ => {
                        lengths.push(0);
                        present.push(false);
                    }
                }
            }
            Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Utf8, true)),
                OffsetBuffer::from_lengths(lengths),
                Arc::new(StringArray::from(flat)),
                Some(NullBuffer::from(present)),
            ))
        }
    }
}

/// Candidate path for heterogeneous metadata: infers a union schema from
/// typed protobuf values (no text), then builds Arrow arrays directly.
pub fn encode_from_proto_dynamic(bytes: &[u8]) -> Result<EncodedBatch, String> {
    let rows = <pb::LogRowsDynamic as prost::Message>::decode(bytes)
        .map_err(|e| e.to_string())?
        .rows;

    let field_schema = infer_schema(&rows);

    let mut fields = vec![timestamp_field(), Field::new("event_message", DataType::Utf8, true)];
    for (name, t) in &field_schema {
        fields.push(Field::new(name, field_data_type(*t), true));
    }
    let schema = Schema::new(fields);

    let timestamp_array =
        TimestampMicrosecondArray::from(rows.iter().map(|r| r.timestamp_us).collect::<Vec<_>>())
            .with_timezone("+00");
    let event_message_array =
        StringArray::from(rows.iter().map(|r| r.event_message.as_str()).collect::<Vec<_>>());

    let mut arrays: Vec<ArrayRef> = vec![Arc::new(timestamp_array), Arc::new(event_message_array)];
    for (name, t) in &field_schema {
        arrays.push(build_field_array(&rows, name, *t));
    }

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays).map_err(|e| e.to_string())?;
    write_ipc(&schema, &batch)
}

pub fn string_value(s: impl Into<String>) -> pb::Value {
    pb::Value {
        kind: Some(pb::value::Kind::StringValue(s.into())),
    }
}

pub fn number_value(n: f64) -> pb::Value {
    pb::Value {
        kind: Some(pb::value::Kind::NumberValue(n)),
    }
}

pub fn list_value(values: Vec<String>) -> pb::Value {
    pb::Value {
        kind: Some(pb::value::Kind::ListValue(pb::StringList { values })),
    }
}

/// Deliberately heterogeneous: keys vary per row (`region`, `latency_ms`,
/// `tags` only appear on some rows), and `count` conflicts in type
/// (number on most rows, string on every 7th) — the exact shape of problem
/// that broke `serde_arrow` and that `infer_json_schema` has to paper over
/// today.
pub fn sample_rows_dynamic(n: usize) -> Vec<pb::LogRowDynamic> {
    (0..n as i64)
        .map(|i| {
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("request_id".to_string(), string_value(format!("req_{i}")));

            if i % 2 == 0 {
                metadata.insert("region".to_string(), string_value("us-east-1"));
            }
            if i % 3 == 0 {
                metadata.insert("latency_ms".to_string(), number_value((i % 800) as f64));
            }
            if i % 7 == 0 {
                metadata.insert("count".to_string(), string_value("unknown"));
            } else {
                metadata.insert("count".to_string(), number_value(i as f64));
            }
            if i % 4 == 0 {
                metadata.insert(
                    "tags".to_string(),
                    list_value(vec!["api".to_string(), "v1".to_string()]),
                );
            }

            pb::LogRowDynamic {
                timestamp_us: 1_700_000_000_000_000 + i,
                event_message: format!("dynamic event {i}"),
                metadata,
            }
        })
        .collect()
}

/// Same heterogeneity (varying keys per row) as `sample_rows_dynamic`, minus
/// the deliberate type conflict on `count` - needed because today's baseline
/// path can't complete at all on that conflict (see the test below), so a
/// fair baseline-vs-candidate speed comparison has to use data the baseline
/// can actually encode.
pub fn sample_rows_dynamic_no_conflict(n: usize) -> Vec<pb::LogRowDynamic> {
    (0..n as i64)
        .map(|i| {
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("request_id".to_string(), string_value(format!("req_{i}")));
            if i % 2 == 0 {
                metadata.insert("region".to_string(), string_value("us-east-1"));
            }
            if i % 3 == 0 {
                metadata.insert("latency_ms".to_string(), number_value((i % 800) as f64));
            }
            metadata.insert("count".to_string(), number_value(i as f64));
            if i % 4 == 0 {
                metadata.insert(
                    "tags".to_string(),
                    list_value(vec!["api".to_string(), "v1".to_string()]),
                );
            }

            pb::LogRowDynamic {
                timestamp_us: 1_700_000_000_000_000 + i,
                event_message: format!("dynamic event {i}"),
                metadata,
            }
        })
        .collect()
}

pub fn rows_to_proto_bytes_dynamic(rows: &[pb::LogRowDynamic]) -> Vec<u8> {
    let msg = pb::LogRowsDynamic { rows: rows.to_vec() };
    <pb::LogRowsDynamic as prost::Message>::encode_to_vec(&msg)
}

/// Baseline-equivalent NDJSON for the same heterogeneous rows, replicating
/// `EventUtils.normalize_df_struct_fields/1`: every row gets every key seen
/// anywhere in the batch, defaulted to `null` where absent, before
/// `Jason.encode!/1` would run.
pub fn rows_to_ndjson_dynamic(rows: &[pb::LogRowDynamic]) -> String {
    let field_schema = infer_schema(rows);

    rows.iter()
        .map(|row| {
            let ts: DateTime<Utc> = DateTime::from_timestamp_micros(row.timestamp_us).unwrap();
            let mut obj = serde_json::Map::new();
            obj.insert(
                "timestamp".to_string(),
                serde_json::Value::String(ts.to_rfc3339_opts(SecondsFormat::Micros, true)),
            );
            obj.insert(
                "event_message".to_string(),
                serde_json::Value::String(row.event_message.clone()),
            );
            for (name, _) in &field_schema {
                let v = match row.metadata.get(name).and_then(|v| v.kind.as_ref()) {
                    Some(pb::value::Kind::StringValue(s)) => serde_json::Value::String(s.clone()),
                    Some(pb::value::Kind::NumberValue(n)) => serde_json::json!(n),
                    Some(pb::value::Kind::BoolValue(b)) => serde_json::Value::Bool(*b),
                    Some(pb::value::Kind::ListValue(l)) => serde_json::json!(l.values),
                    None => serde_json::Value::Null,
                };
                obj.insert(name.clone(), v);
            }
            serde_json::Value::Object(obj).to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decode_arrow, encode_from_json};

    #[test]
    fn handles_missing_keys_and_type_conflicts_without_error() {
        let rows = sample_rows_dynamic(100);
        let proto_bytes = rows_to_proto_bytes_dynamic(&rows);

        let candidate = encode_from_proto_dynamic(&proto_bytes).expect("candidate path failed");
        let decoded = decode_arrow(&candidate).expect("decode candidate");

        assert_eq!(decoded.len(), 100);

        // "count" conflicts in type somewhere in the batch (row 0, 7, 14...
        // are strings; the rest are numbers), so the whole column widens to
        // Utf8 - every row's value is stringified, not just the conflicting
        // ones. This is a per-column policy, not a per-value one.
        assert_eq!(decoded[0]["count"], "unknown");
        assert_eq!(decoded[1]["count"], "1");
        // row 1: i % 2 != 0 -> "region" absent entirely for this row
        assert!(decoded[1].get("region").map(|v| v.is_null()).unwrap_or(true));
        // row 0: i % 2 == 0 -> "region" present
        assert_eq!(decoded[0]["region"], "us-east-1");
        // row 4: i % 4 == 0 -> "tags" present as a list
        assert_eq!(decoded[4]["tags"], serde_json::json!(["api", "v1"]));
    }

    #[test]
    fn candidate_and_baseline_agree_on_heterogeneous_but_non_conflicting_data() {
        let rows = sample_rows_dynamic_no_conflict(300);
        let proto_bytes = rows_to_proto_bytes_dynamic(&rows);
        let ndjson = rows_to_ndjson_dynamic(&rows);

        let candidate = encode_from_proto_dynamic(&proto_bytes).expect("candidate failed");
        let baseline = encode_from_json(&ndjson).expect("baseline failed");

        let candidate_rows = decode_arrow(&candidate).expect("decode candidate");
        let baseline_rows = decode_arrow(&baseline).expect("decode baseline");

        assert_eq!(
            candidate_rows, baseline_rows,
            "candidate and baseline disagree on heterogeneous (but type-consistent) data"
        );
    }

    /// Not an equality assertion against the candidate — this documents what
    /// today's actual production path (JSON + `infer_json_schema`) does when
    /// faced with the same type conflict, so the comparison is informed
    /// rather than assumed.
    #[test]
    fn baseline_behavior_on_the_same_type_conflict_is_observed_directly() {
        let rows = sample_rows_dynamic(100);
        let ndjson = rows_to_ndjson_dynamic(&rows);

        let result = encode_from_json(&ndjson);
        match result {
            Ok(baseline) => {
                let decoded = decode_arrow(&baseline).expect("decode baseline");
                println!(
                    "baseline succeeded on conflicting types; row 0 'count' = {:?}, row 1 'count' = {:?}",
                    decoded[0].get("count"),
                    decoded[1].get("count")
                );
            }
            Err(e) => {
                println!("baseline (today's JSON + infer_json_schema path) FAILED on conflicting types: {e}");
            }
        }
    }
}
