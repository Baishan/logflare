//! Tests whether building the protobuf bytes in a NIF beats Elixir's own
//! `protobuf` library (measured in bench/bq_storage_write_elixir_encode.exs,
//! which found only a ~1.1x edge over Jason/NDJSON - the smallest win of the
//! four measurements). Same risk shape as the existing `arrowipc_ex`
//! NIF: synchronous, `DirtyCpu`, no async runtime, no long-lived state - none
//! of the sidecar-motivating risk applies to a NIF this small and stateless.
//!
//! Takes rows as plain tuples rather than maps so Rustler's built-in
//! `Decoder` impls (for tuples, `String`, `i64`, `Vec<T>`) do all the
//! decoding - no custom term-decoding code needed.

use crate::pb;
use crate::EncodedBatch;
use rustler::{Binary, Env, Error, NifResult, OwnedBinary};

#[rustler::nif(schedule = "DirtyCpu")]
fn encode_rows_to_protobuf<'a>(
    env: Env<'a>,
    rows: Vec<(i64, String, String, i64, String, Vec<String>)>,
) -> Binary<'a> {
    let log_rows = pb::LogRows {
        rows: rows
            .into_iter()
            .map(|(timestamp_us, event_message, request_id, user_id, user_plan, tags)| pb::LogRow {
                timestamp_us,
                event_message,
                request_id,
                user: Some(pb::UserInfo { id: user_id, plan: user_plan }),
                tags,
            })
            .collect(),
    };
    let bytes = <pb::LogRows as prost::Message>::encode_to_vec(&log_rows);

    let mut binary = OwnedBinary::new(bytes.len()).unwrap();
    binary.as_mut_slice().copy_from_slice(&bytes);
    binary.release(env)
}

/// Test-only utility: decodes Arrow IPC bytes (from either NIF path) back
/// into a comparable JSON string, so correctness can be checked from Elixir
/// against the real production baseline without re-implementing a decoder
/// there. Not part of the actual candidate pipeline.
#[rustler::nif(schedule = "DirtyCpu")]
fn decode_arrow_ipc_to_json(schema: Binary, batches: Vec<Binary>) -> NifResult<String> {
    let encoded = EncodedBatch {
        schema: schema.as_slice().to_vec(),
        batches: batches.iter().map(|b| b.as_slice().to_vec()).collect(),
    };
    let rows = crate::decode_arrow(&encoded).map_err(|e| Error::Term(Box::new(e)))?;
    serde_json::to_string(&rows).map_err(|e| Error::Term(Box::new(e.to_string())))
}

rustler::init!("Elixir.Bench.BqArrowPocNif");
