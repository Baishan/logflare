# Usage: mix run --no-start bench/bq_storage_write_ipc_hop.exs
#
# Measures the one cost neither of the other two bq_storage_write_* benches
# cover: an Elixir -> local sidecar HTTP round-trip, in isolation from any
# encoding or BigQuery work. The mock sidecar (native/bq_arrow_poc, binary
# `mock_sidecar`) just reads the body and replies with a tiny fixed ack, so
# whatever time shows up here is transport overhead only - not a substitute
# for the real sidecar's actual processing time, which is measured
# separately in native/bq_arrow_poc/benches/encode_comparison.rs.

sidecar_bin =
  Path.join([__DIR__, "..", "native", "bq_arrow_poc", "target", "release", "mock_sidecar"])
  |> Path.expand()

unless File.exists?(sidecar_bin) do
  raise """
  #{sidecar_bin} not found.
  Build it first: cd native/bq_arrow_poc && PROTOC=$(which protoc) cargo build --release --bin mock_sidecar
  """
end

port = 8933 + :rand.uniform(1000)
base_url = "http://127.0.0.1:#{port}/append"

sidecar_port = Port.open({:spawn_executable, sidecar_bin}, [
  :binary,
  :exit_status,
  args: [],
  env: [{~c"MOCK_SIDECAR_PORT", String.to_charlist("#{port}")}]
])

# Wait for the sidecar's "READY" line instead of guessing with a fixed sleep.
receive do
  {^sidecar_port, {:data, "READY" <> _}} -> :ok
after
  5_000 -> raise "mock_sidecar did not start within 5s"
end

{:ok, _} = Application.ensure_all_started(:finch)
{:ok, _} = Finch.start_link(name: BenchFinch, pools: %{default: [size: 10]})

payload_for = fn bytes -> :crypto.strong_rand_bytes(bytes) end

# Sizes matched to the protobuf payload sizes measured in
# bq_storage_write_elixir_encode.exs for 50/500/2000-row batches.
payloads = %{
  "~50 rows (3.7 KB)" => payload_for.(3_725),
  "~500 rows (39 KB)" => payload_for.(39_051),
  "~2000 rows (159 KB)" => payload_for.(159_333)
}

post = fn body ->
  req = Finch.build(:post, base_url, [{"content-type", "application/x-protobuf"}], body)
  {:ok, %Finch.Response{status: 200}} = Finch.request(req, BenchFinch)
end

# Sanity check before measuring.
for {label, body} <- payloads, do: post.(body) && IO.puts("#{label}: round-trip ok")
IO.puts("")

Benchee.run(
  %{"local_http_round_trip" => fn body -> post.(body) end},
  inputs: payloads,
  time: 5,
  warmup: 2,
  print: [configuration: false]
)

Port.close(sidecar_port)
