//! Spike: compares two ways of turning log rows into the Arrow IPC bytes the
//! BigQuery Storage Write API requires.
//!
//! - `encode_from_json` mirrors today's production path
//!   (`native/arrowipc_ex`): JSON text -> `infer_json_schema` -> re-parse the
//!   same text with `ReaderBuilder`. Two full text passes.
//! - `encode_from_proto` is the candidate path: protobuf bytes -> typed
//!   values -> Arrow arrays built directly. Zero text passes.
//!
//! Scope cut: `encode_from_proto` targets one fixed row shape (see
//! `proto/rows.proto`) rather than arbitrary/dynamic metadata. Real metadata
//! is dynamic, which is exactly why the production path needs JSON + schema
//! inference in the first place; a generic version of this approach needs
//! either one schema per known log shape, or a generic typed Value/Struct
//! message with matching generic (but still text-free) Arrow-building code.
//! That's follow-up work, not something this spike claims to solve.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow::json::reader::{infer_json_schema, ReaderBuilder};
use chrono::{DateTime, SecondsFormat, Utc};

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/logflare.poc.rs"));
}

pub mod dynamic;
pub mod nif;
pub mod nif_arrow;

/// Arrow IPC bytes for one schema message plus one or more record-batch
/// messages, matching the shape `GoogleApiClient.encode_arrow_data/1`
/// currently builds from `ArrowIPC.get_ipc_bytes/1`.
#[derive(Debug, Clone)]
pub struct EncodedBatch {
    pub schema: Vec<u8>,
    pub batches: Vec<Vec<u8>>,
}

pub(crate) fn timestamp_field() -> Field {
    Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, Some("+00".into())),
        false,
    )
}

fn user_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("plan", DataType::Utf8, true),
    ])
}

/// The schema shared by both paths. Both MUST agree on this for the
/// correctness check (`decode_arrow` equivalence) to mean anything.
fn target_schema() -> Schema {
    Schema::new(vec![
        timestamp_field(),
        Field::new("event_message", DataType::Utf8, true),
        Field::new("request_id", DataType::Utf8, true),
        Field::new(
            "user",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(user_struct_fields()),
                true,
            ))),
            true,
        ),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ])
}

pub(crate) fn write_ipc(schema: &Schema, batch: &RecordBatch) -> Result<EncodedBatch, String> {
    let write_options = IpcWriteOptions::default();
    let data_gen = IpcDataGenerator::default();

    let schema_bytes = {
        let mut tracker = DictionaryTracker::new(false);
        let schema_data =
            data_gen.schema_to_bytes_with_dictionary_tracker(schema, &mut tracker, &write_options);
        let mut out = Vec::new();
        write_message(&mut out, schema_data, &write_options).map_err(|e| e.to_string())?;
        out
    };

    let batch_bytes = {
        let mut tracker = DictionaryTracker::new(false);
        let (_, encoded) = data_gen
            .encoded_batch(batch, &mut tracker, &write_options)
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        write_message(&mut out, encoded, &write_options).map_err(|e| e.to_string())?;
        out
    };

    Ok(EncodedBatch {
        schema: schema_bytes,
        batches: vec![batch_bytes],
    })
}

/// Candidate path: protobuf-encoded `LogRows` -> Arrow IPC bytes, with no
/// text parsing anywhere in between.
pub fn encode_from_proto(bytes: &[u8]) -> Result<EncodedBatch, String> {
    let rows = <pb::LogRows as prost::Message>::decode(bytes).map_err(|e| e.to_string())?.rows;

    let timestamp_array = TimestampMicrosecondArray::from(
        rows.iter().map(|r| r.timestamp_us).collect::<Vec<_>>(),
    )
    .with_timezone("+00");

    let event_message_array =
        StringArray::from(rows.iter().map(|r| r.event_message.as_str()).collect::<Vec<_>>());

    let request_id_array =
        StringArray::from(rows.iter().map(|r| r.request_id.as_str()).collect::<Vec<_>>());

    // `user` is a single nested message per row in the proto, but BigQuery
    // represents a nested object as a REPEATED RECORD column, so (mirroring
    // `EventUtils.wrap_body_fields/1`) each row's struct is wrapped in a
    // length-1 list.
    let user_ids: Int64Array = rows
        .iter()
        .map(|r| r.user.as_ref().map(|u| u.id))
        .collect();
    let user_plans: StringArray = rows
        .iter()
        .map(|r| r.user.as_ref().map(|u| u.plan.as_str()))
        .collect();
    let user_struct = StructArray::new(
        user_struct_fields(),
        vec![Arc::new(user_ids) as ArrayRef, Arc::new(user_plans) as ArrayRef],
        None,
    );
    let user_list = ListArray::new(
        Arc::new(Field::new("item", DataType::Struct(user_struct_fields()), true)),
        OffsetBuffer::from_lengths(rows.iter().map(|_| 1usize)),
        Arc::new(user_struct),
        None,
    );

    // `tags` is already a repeated scalar in both the proto and the source
    // data, so it needs no wrapping, just flattening into one child array
    // plus per-row offsets.
    let flat_tags = StringArray::from(
        rows.iter()
            .flat_map(|r| r.tags.iter().map(String::as_str))
            .collect::<Vec<_>>(),
    );
    let tags_list = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::from_lengths(rows.iter().map(|r| r.tags.len())),
        Arc::new(flat_tags),
        None,
    );

    let schema = target_schema();
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(timestamp_array),
            Arc::new(event_message_array),
            Arc::new(request_id_array),
            Arc::new(user_list),
            Arc::new(tags_list),
        ],
    )
    .map_err(|e| e.to_string())?;

    write_ipc(&schema, &batch)
}

/// Baseline path, mirroring `native/arrowipc_ex/src/lib.rs::get_ipc_bytes`:
/// infer a schema from JSON text, then re-parse the same text to build the
/// record batch. Takes already-serialized NDJSON, same as the real
/// `ArrowIPC.get_ipc_bytes/1` NIF does.
pub fn encode_from_json(ndjson: &str) -> Result<EncodedBatch, String> {
    let byte_data = ndjson.as_bytes();

    let inferred_schema = {
        let (inferred, _) = infer_json_schema(byte_data, None).map_err(|e| e.to_string())?;
        let fields: Vec<Arc<Field>> = inferred
            .fields()
            .iter()
            .map(|field| {
                if field.name() == "timestamp" {
                    Arc::new(timestamp_field())
                } else {
                    field.clone()
                }
            })
            .collect();
        Arc::new(Schema::new(fields))
    };

    let reader = ReaderBuilder::new(inferred_schema.clone())
        .build(byte_data)
        .map_err(|e| e.to_string())?;

    let mut encoded_batches = Vec::new();
    let write_options = IpcWriteOptions::default();
    let data_gen = IpcDataGenerator::default();

    let schema_bytes = {
        let mut tracker = DictionaryTracker::new(false);
        let schema_data = data_gen.schema_to_bytes_with_dictionary_tracker(
            &inferred_schema,
            &mut tracker,
            &write_options,
        );
        let mut out = Vec::new();
        write_message(&mut out, schema_data, &write_options).map_err(|e| e.to_string())?;
        out
    };

    for batch_result in reader {
        let batch = batch_result.map_err(|e| e.to_string())?;
        let mut tracker = DictionaryTracker::new(false);
        let (_, encoded) = data_gen
            .encoded_batch(&batch, &mut tracker, &write_options)
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        write_message(&mut out, encoded, &write_options).map_err(|e| e.to_string())?;
        encoded_batches.push(out);
    }

    Ok(EncodedBatch {
        schema: schema_bytes,
        batches: encoded_batches,
    })
}

/// Formats a `LogRow` the way `EventUtils.log_event_to_df_struct/1` +
/// `wrap_body_fields/1` + `Jason.encode!/1` would, including the
/// stringified timestamp (Jason encodes an Elixir `DateTime` as an RFC3339
/// string, which is why `get_ipc_bytes` needs its "timestamp" field
/// override).
pub fn row_to_json_value(row: &pb::LogRow) -> serde_json::Value {
    let ts: DateTime<Utc> = DateTime::from_timestamp_micros(row.timestamp_us).unwrap();

    serde_json::json!({
        "timestamp": ts.to_rfc3339_opts(SecondsFormat::Micros, true),
        "event_message": row.event_message,
        "request_id": row.request_id,
        "user": [{
            "id": row.user.as_ref().map(|u| u.id),
            "plan": row.user.as_ref().map(|u| u.plan.clone()),
        }],
        "tags": row.tags,
    })
}

pub fn rows_to_ndjson(rows: &[pb::LogRow]) -> String {
    rows.iter()
        .map(|r| row_to_json_value(r).to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn rows_to_proto_bytes(rows: &[pb::LogRow]) -> Vec<u8> {
    let msg = pb::LogRows { rows: rows.to_vec() };
    <pb::LogRows as prost::Message>::encode_to_vec(&msg)
}

/// Decodes Arrow IPC bytes back into comparable JSON rows, standing in for
/// "what BigQuery would see" without needing a real or mocked BigQuery.
pub fn decode_arrow(encoded: &EncodedBatch) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, String> {
    let mut buf = encoded.schema.clone();
    for batch in &encoded.batches {
        buf.extend_from_slice(batch);
    }

    let reader = StreamReader::try_new(Cursor::new(buf), None).map_err(|e| e.to_string())?;
    let mut out_buf = Vec::new();
    {
        let mut writer = arrow::json::writer::LineDelimitedWriter::new(&mut out_buf);
        for batch_result in reader {
            let batch = batch_result.map_err(|e| e.to_string())?;
            writer.write(&batch).map_err(|e| e.to_string())?;
        }
        writer.finish().map_err(|e| e.to_string())?;
    }

    String::from_utf8(out_buf)
        .map_err(|e| e.to_string())?
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(line)
                .map_err(|e| e.to_string())
        })
        .collect()
}

/// Realistic-ish sample data: uniform schema across rows (every row has the
/// same fields), varied values — representative of a single source's steady
/// stream, not of schema drift across sources (out of scope here, see the
/// module doc).
pub fn sample_rows(n: usize) -> Vec<pb::LogRow> {
    (0..n as i64)
        .map(|i| pb::LogRow {
            timestamp_us: 1_700_000_000_000_000 + i,
            event_message: format!("request {i} completed in {}ms", i % 800),
            request_id: format!("req_{i}"),
            user: Some(pb::UserInfo {
                id: i % 100_000,
                plan: ["free", "pro", "enterprise"][(i % 3) as usize].to_string(),
            }),
            tags: vec![
                "api".to_string(),
                "v1".to_string(),
                ["GET", "POST", "PUT"][(i % 3) as usize].to_string(),
            ],
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_and_baseline_produce_equivalent_data() {
        let rows = sample_rows(200);

        let proto_bytes = rows_to_proto_bytes(&rows);
        let ndjson = rows_to_ndjson(&rows);

        let candidate = encode_from_proto(&proto_bytes).expect("candidate path failed");
        let baseline = encode_from_json(&ndjson).expect("baseline path failed");

        let candidate_rows = decode_arrow(&candidate).expect("decode candidate");
        let baseline_rows = decode_arrow(&baseline).expect("decode baseline");

        assert_eq!(candidate_rows.len(), rows.len());
        assert_eq!(
            candidate_rows, baseline_rows,
            "candidate and baseline paths produced different Arrow data for the same input rows"
        );
    }

    #[test]
    fn nested_and_repeated_fields_round_trip() {
        let rows = sample_rows(5);
        let proto_bytes = rows_to_proto_bytes(&rows);
        let candidate = encode_from_proto(&proto_bytes).unwrap();
        let decoded = decode_arrow(&candidate).unwrap();

        let user = decoded[2]["user"].as_array().expect("user is a list");
        assert_eq!(user.len(), 1, "nested map wraps to a length-1 list");
        assert_eq!(user[0]["plan"], "enterprise");

        let tags = decoded[2]["tags"].as_array().expect("tags is a list");
        assert_eq!(tags.len(), 3);
    }
}
