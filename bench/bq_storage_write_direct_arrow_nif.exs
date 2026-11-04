# Usage: mix run --no-start bench/bq_storage_write_direct_arrow_nif.exs
#
# Tests the merged candidate: one NIF taking the *raw* LogEvent.body map
# directly and producing Arrow IPC bytes, with no JSON and no protobuf
# intermediate. Replaces, in one Rust pass, what four Elixir-side steps do
# today: EventUtils.log_event_to_df_struct/1 (DateTime conversion),
# wrap_body_fields/1 (nested-map-as-list), normalize_df_struct_fields/1
# (cross-row key union), and GoogleApiClient.encode_ndjson/1 (Jason).
#
# Compression is forced to :none on the baseline so this measures parsing +
# building only, not a compression-algorithm difference.

Code.require_file("support/bq_arrow_poc_nif.ex", __DIR__)

alias Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC
alias Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient
alias Logflare.Google.BigQuery.EventUtils
alias Logflare.LogEvent

tag = System.get_env("TAG", "untagged")
save_dir = System.get_env("BENCH_SAVE_DIR", "/tmp")
File.mkdir_p!(save_dir)

fixed_body = fn i ->
  %{
    "timestamp" => 1_700_000_000_000_000 + i,
    "event_message" => "request #{i} completed in #{rem(i, 800)}ms",
    "request_id" => "req_#{i}",
    "user" => %{"id" => rem(i, 100_000), "plan" => Enum.at(~w(free pro enterprise), rem(i, 3))},
    "tags" => ["api", "v1", Enum.at(~w(GET POST PUT), rem(i, 3))]
  }
end

heterogeneous_body = fn i ->
  base = %{
    "timestamp" => 1_700_000_000_000_000 + i,
    "event_message" => "dynamic event #{i}",
    "request_id" => "req_#{i}",
    "count" => i * 1.0
  }

  base
  |> then(&if rem(i, 2) == 0, do: Map.put(&1, "region", "us-east-1"), else: &1)
  |> then(&if rem(i, 3) == 0, do: Map.put(&1, "latency_ms", rem(i, 800) * 1.0), else: &1)
  |> then(&if rem(i, 4) == 0, do: Map.put(&1, "tags", ["api", "v1"]), else: &1)
end

log_event = fn body_fn, i ->
  %LogEvent{
    id: Ecto.UUID.generate(),
    source_uuid: :bench_source,
    source_name: "bench source",
    event_type: :log,
    ingested_at: DateTime.utc_now(),
    body: body_fn.(i)
  }
end

encode_baseline = fn log_events ->
  ndjson =
    log_events
    |> Enum.map(&EventUtils.log_event_to_df_struct/1)
    |> EventUtils.normalize_df_struct_fields()
    |> GoogleApiClient.encode_ndjson()

  ArrowIPC.get_ipc_bytes(ndjson, :none)
end

encode_candidate = fn log_events ->
  rows = Enum.map(log_events, & &1.body)
  Bench.BqArrowPocNif.encode_rows_to_arrow_ipc(rows)
end

decode = fn {schema, batches} ->
  Bench.BqArrowPocNif.decode_arrow_ipc_to_json(schema, batches) |> Jason.decode!()
end

IO.puts("=== correctness ===")

for {label, body_fn} <- [{"fixed schema", fixed_body}, {"heterogeneous schema", heterogeneous_body}] do
  log_events = Enum.map(1..200, &log_event.(body_fn, &1))

  baseline_decoded = encode_baseline.(log_events) |> decode.()
  candidate_decoded = encode_candidate.(log_events) |> decode.()

  if baseline_decoded == candidate_decoded do
    IO.puts("#{label}: MATCH (200 rows)")
  else
    diff_index = Enum.find_index(Enum.zip(baseline_decoded, candidate_decoded), fn {a, b} -> a != b end)

    IO.puts("#{label}: MISMATCH at row #{diff_index}")
    IO.inspect(Enum.at(baseline_decoded, diff_index || 0), label: "baseline")
    IO.inspect(Enum.at(candidate_decoded, diff_index || 0), label: "candidate")
  end
end

IO.puts("")
IO.puts("=== speed ===")

for {label, body_fn} <- [{"fixed", fixed_body}, {"heterogeneous", heterogeneous_body}] do
  inputs =
    for batch_size <- [50, 500, 2000], into: %{} do
      {"#{label} #{batch_size} rows", Enum.map(1..batch_size, &log_event.(body_fn, &1))}
    end

  Benchee.run(
    %{
      "baseline_jason_ndjson_arrow" => fn log_events -> encode_baseline.(log_events) end,
      "candidate_direct_nif" => fn log_events -> encode_candidate.(log_events) end
    },
    inputs: inputs,
    time: 5,
    warmup: 2,
    memory_time: 2,
    save: [path: Path.join(save_dir, "bq_direct_nif_#{label}_#{tag}.benchee"), tag: tag],
    print: [configuration: false]
  )
end
