defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.KafkaSerializer do
  @moduledoc false

  alias Logflare.LogEvent

  @spec encode(LogEvent.t()) :: binary()
  def encode(%LogEvent{} = le) do
    Jason.encode!(%{id: le.id, body: le.body})
  end

  @spec decode(binary()) :: %{String.t() => term()}
  def decode(binary) do
    Jason.decode!(binary)
  end
end
