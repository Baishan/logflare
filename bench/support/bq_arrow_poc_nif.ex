# Hand-loaded NIF (bypassing the `Rustler` Hex macro / mix.exs wiring) so this
# spike stays fully isolated from the main app's build. Mirrors what
# `use Rustler, otp_app: ..., crate: ...` generates under the hood.
defmodule Bench.BqArrowPocNif do
  @on_load :load_nif

  def load_nif do
    path =
      Path.join([__DIR__, "..", "..", "native", "bq_arrow_poc", "target", "release", "libbq_arrow_poc"])
      |> Path.expand()
      |> String.to_charlist()

    :erlang.load_nif(path, 0)
  end

  def encode_rows_to_protobuf(_rows), do: :erlang.nif_error(:nif_not_loaded)
  def encode_rows_to_arrow_ipc(_rows), do: :erlang.nif_error(:nif_not_loaded)
  def decode_arrow_ipc_to_json(_schema, _batches), do: :erlang.nif_error(:nif_not_loaded)
end
