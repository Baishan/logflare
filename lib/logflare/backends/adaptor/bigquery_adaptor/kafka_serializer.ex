defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.KafkaSerializer do
  @moduledoc false

  alias Logflare.LogEvent

  @spec encode(LogEvent.t(), pos_integer() | nil) :: binary()
  def encode(%LogEvent{} = le, backend_id) do
    Jason.encode!(%{
      id: le.id,
      body: le.body,
      source_id: le.source_id,
      backend_id: backend_id,
      event_type: le.event_type,
      ingested_at: le.ingested_at
    })
  end

  @spec decode(binary()) :: {:ok, %{String.t() => term()}} | {:error, Jason.DecodeError.t()}
  def decode(binary) do
    Jason.decode(binary)
  end
end
