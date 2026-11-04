# Usage: mix run --no-start bench/bq_storage_write_explorer_comparison.exs
#
# Checks whether Explorer (already a Logflare dependency, ~> 0.11.1) can
# replace the custom arrowipc_ex NIF, using its own dump_ipc_schema/
# dump_ipc_record_batch - added in 0.11.1 specifically, producing the same
# schema-message + record-batch-message shape arrowipc_ex hand-builds.
# Keeps the existing EventUtils preprocessing (log_event_to_df_struct,
# wrap_body_fields, normalize_df_struct_fields) for all three paths, since
# Explorer requires uniform columns across rows just like today's pipeline
# does - this isolates the comparison to "which library builds the Arrow IPC
# bytes," not "do we still need to normalize first."

Code.require_file("support/bq_arrow_poc_nif.ex", __DIR__)

alias Explorer.DataFrame
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

log_event = fn i ->
  %LogEvent{
    id: Ecto.UUID.generate(),
    source_uuid: :bench_source,
    source_name: "bench source",
    event_type: :log,
    ingested_at: DateTime.utc_now(),
    body: fixed_body.(i)
  }
end

normalized = fn log_events ->
  log_events
  |> Enum.map(&EventUtils.log_event_to_df_struct/1)
  |> EventUtils.normalize_df_struct_fields()
end

encode_baseline = fn log_events ->
  ndjson = normalized.(log_events) |> GoogleApiClient.encode_ndjson()
  ArrowIPC.get_ipc_bytes(ndjson, :none)
end

encode_explorer = fn log_events ->
  df = normalized.(log_events) |> DataFrame.new()
  {:ok, schema} = DataFrame.dump_ipc_schema(df)
  {:ok, batches} = DataFrame.dump_ipc_record_batch(df, compression: nil)
  {schema, batches}
end

encode_nif = fn log_events ->
  Bench.BqArrowPocNif.encode_rows_to_arrow_ipc(Enum.map(log_events, & &1.body))
end

decode = fn {schema, batches} ->
  Bench.BqArrowPocNif.decode_arrow_ipc_to_json(schema, batches) |> Jason.decode!()
end

IO.puts("=== correctness ===")
log_events = Enum.map(1..200, log_event)

baseline_decoded = encode_baseline.(log_events) |> decode.()
explorer_decoded = encode_explorer.(log_events) |> decode.()

if baseline_decoded == explorer_decoded do
  IO.puts("baseline vs explorer: MATCH (200 rows)")
else
  idx = Enum.find_index(Enum.zip(baseline_decoded, explorer_decoded), fn {a, b} -> a != b end)
  IO.puts("baseline vs explorer: MISMATCH at row #{idx}")
  IO.inspect(Enum.at(baseline_decoded, idx), label: "baseline")
  IO.inspect(Enum.at(explorer_decoded, idx), label: "explorer")
end

IO.puts("")
IO.puts("=== speed ===")

inputs =
  for batch_size <- [50, 500, 2000], into: %{} do
    {"#{batch_size} rows", Enum.map(1..batch_size, log_event)}
  end

Benchee.run(
  %{
    "baseline_jason_ndjson_arrow" => fn log_events -> encode_baseline.(log_events) end,
    "explorer_dataframe" => fn log_events -> encode_explorer.(log_events) end,
    "candidate_direct_nif" => fn log_events -> encode_nif.(log_events) end
  },
  inputs: inputs,
  time: 5,
  warmup: 2,
  memory_time: 2,
  save: [path: Path.join(save_dir, "bq_explorer_#{tag}.benchee"), tag: tag],
  print: [configuration: false]
)
