# Usage: mix run --no-start bench/bq_storage_write_elixir_encode.exs
#
# Measures the Elixir-side serialization cost that the Rust-side benchmark in
# native/bq_arrow_poc/benches/encode_comparison.rs doesn't cover: building the
# NDJSON string (today's real path, via the actual production functions in
# Logflare.Google.BigQuery.EventUtils and GoogleApiClient) vs. building and
# encoding the equivalent protobuf message (the candidate path). Both start
# from the same raw LogEvent body shape.

Code.require_file("support/bq_arrow_poc_pb/rows.pb.ex", __DIR__)
Code.require_file("support/bq_arrow_poc_nif.ex", __DIR__)

alias Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient
alias Logflare.Google.BigQuery.EventUtils
alias Logflare.LogEvent
alias Logflare.Poc.LogRow
alias Logflare.Poc.LogRows
alias Logflare.Poc.UserInfo

tag = System.get_env("TAG", "untagged")
save_dir = System.get_env("BENCH_SAVE_DIR", "/tmp")
File.mkdir_p!(save_dir)

raw_body = fn i ->
  %{
    "timestamp" => 1_700_000_000_000_000 + i,
    "event_message" => "request #{i} completed in #{rem(i, 800)}ms",
    "request_id" => "req_#{i}",
    "user" => %{"id" => rem(i, 100_000), "plan" => Enum.at(~w(free pro enterprise), rem(i, 3))},
    "tags" => ["api", "v1", Enum.at(~w(GET POST PUT), rem(i, 3))]
  }
end

log_event = fn i ->
  %LogEvent{
    id: Ecto.UUID.generate(),
    source_uuid: :bench_source,
    source_name: "bench source",
    event_type: :log,
    ingested_at: DateTime.utc_now(),
    body: raw_body.(i)
  }
end

# Baseline: the real production path, unmodified.
encode_baseline = fn log_events ->
  log_events
  |> Enum.map(&EventUtils.log_event_to_df_struct/1)
  |> EventUtils.normalize_df_struct_fields()
  |> GoogleApiClient.encode_ndjson()
end

# Candidate: build the protobuf struct directly from the raw body - no
# DateTime conversion, no wrap_body_fields, no Jason.
encode_candidate = fn log_events ->
  rows =
    Enum.map(log_events, fn %LogEvent{body: body} ->
      struct!(LogRow,
        timestamp_us: body["timestamp"],
        event_message: body["event_message"],
        request_id: body["request_id"],
        user: struct!(UserInfo, id: body["user"]["id"], plan: body["user"]["plan"]),
        tags: body["tags"]
      )
    end)

  LogRows.encode(struct!(LogRows, rows: rows))
end

# Candidate variant: same protobuf bytes, built in Rust instead of via
# Elixir's `protobuf` library. Rows are passed as flat tuples so Rustler's
# built-in decoders (no custom term-decoding code) handle the argument.
encode_candidate_nif = fn log_events ->
  rows =
    Enum.map(log_events, fn %LogEvent{body: body} ->
      {body["timestamp"], body["event_message"], body["request_id"], body["user"]["id"],
       body["user"]["plan"], body["tags"]}
    end)

  Bench.BqArrowPocNif.encode_rows_to_protobuf(rows)
end

for batch_size <- [50, 500, 2000] do
  log_events = Enum.map(1..batch_size, log_event)

  baseline_bytes = encode_baseline.(log_events) |> byte_size()
  candidate_bytes = encode_candidate.(log_events) |> byte_size()
  candidate_nif_bytes = encode_candidate_nif.(log_events) |> byte_size()

  if candidate_bytes != candidate_nif_bytes do
    raise "NIF and Elixir protobuf encoders disagree on output size (#{candidate_nif_bytes} vs #{candidate_bytes})"
  end

  IO.puts(
    "batch=#{batch_size}: baseline (ndjson) #{baseline_bytes} bytes, " <>
      "candidate (protobuf) #{candidate_bytes} bytes " <>
      "(#{Float.round(candidate_bytes / baseline_bytes * 100, 1)}% of baseline size)"
  )
end

IO.puts("")

inputs =
  for batch_size <- [50, 500, 2000], into: %{} do
    {"#{batch_size} rows", Enum.map(1..batch_size, log_event)}
  end

Benchee.run(
  %{
    "baseline_jason_ndjson" => fn log_events -> encode_baseline.(log_events) end,
    "candidate_protobuf_elixir" => fn log_events -> encode_candidate.(log_events) end,
    "candidate_protobuf_nif" => fn log_events -> encode_candidate_nif.(log_events) end
  },
  inputs: inputs,
  time: 5,
  warmup: 2,
  memory_time: 2,
  save: [path: Path.join(save_dir, "bq_elixir_encode_#{tag}.benchee"), tag: tag],
  print: [configuration: false]
)
