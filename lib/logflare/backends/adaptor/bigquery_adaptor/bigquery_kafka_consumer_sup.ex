defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.BigQueryKafkaConsumerSup do
  @moduledoc """
  Supervisor that starts a single global KafkaConsumerPipeline for BigQuery.

  One consumer group reads the shared Kafka topic and routes events to the
  correct BigQuery table via `put_batch_key/1`. This is started once at
  application startup (not per-source or per-backend).
  """

  use Supervisor

  alias Logflare.Backends.Adaptor.BigQueryAdaptor.KafkaConsumerPipeline

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(_opts) do
    children = [
      {KafkaConsumerPipeline, [name: KafkaConsumerPipeline]}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
