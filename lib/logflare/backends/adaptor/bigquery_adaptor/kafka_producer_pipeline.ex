defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.KafkaProducerPipeline do
  @moduledoc false

  use Broadway

  require Logger

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.KafkaSerializer
  alias Logflare.Backends.BufferProducer

  @behaviour Broadway.Acknowledger

  @max_retries 0

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    {name, args} = Keyword.pop!(args, :name)
    source = Keyword.fetch!(args, :source)
    backend = Keyword.fetch!(args, :backend)

    kafka_config = Application.get_env(:logflare, :kafka, [])
    topic = Keyword.fetch!(kafka_config, :topic)
    partitions = Keyword.get(kafka_config, :partitions, 1)

    ack_config = %{max_retries: @max_retries}

    opts = [
      name: name,
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 10],
      producer: [
        module:
          {BufferProducer,
           [
             source_id: source.id,
             backend_id: backend.id,
             id_passing: true
           ]},
        transformer:
          {__MODULE__, :transform,
           [
             ref: {{source.id, backend.id, nil}, ack_config}
           ]}
      ],
      processors: [
        default: [concurrency: 8, max_demand: 1_000]
      ],
      batchers: [
        kafka: [
          concurrency: 4,
          batch_size: 500,
          batch_timeout: 500
        ]
      ],
      context: %{
        topic: topic,
        partitions: partitions,
        source_token: source.token,
        source_id: source.id,
        backend_id: backend.id
      }
    ]

    Broadway.start_link(__MODULE__, opts)
  end

  @impl Broadway
  def process_name({:via, module, {registry, identifier}}, base_name) do
    {:via, module, {registry, {identifier, base_name}}}
  end

  def process_name(proc_name, base_name) do
    String.to_atom("#{proc_name}-#{base_name}")
  end

  def transform(event, args) do
    ref = args[:ref]

    %Message{
      data: event,
      acknowledger: {__MODULE__, ref, :ack_data}
    }
  end

  @impl Broadway.Acknowledger
  def ack({_queue, config}, successful, failed) do
    maybe_requeue_failed(failed, config)

    # When max_retries is 0 we drop failed messages without requeue.
    # Still delete them from ETS so they don't stall the queue indefinitely.
    to_delete =
      case config do
        %{max_retries: 0} -> successful ++ failed
        _ -> successful
      end

    for %{data: {id, tid}} <- to_delete do
      :ets.delete(tid, id)
    end

    :ok
  end

  @impl Broadway
  def handle_message(_processor, message, _context) do
    Message.put_batcher(message, :kafka)
  end

  @impl Broadway
  def handle_batch(:kafka, messages, batch_info, context) do
    case produce_to_kafka(
           messages,
           context.topic,
           context.source_token,
           context.partitions,
           context.backend_id
         ) do
      :ok ->
        :telemetry.execute(
          [:logflare, :backends, :pipeline, :handle_batch],
          %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
          %{backend_type: :bigquery_kafka_producer}
        )

        messages

      {:error, reason} ->
        Logger.warning("Failed to produce to Kafka, failing batch",
          error: inspect(reason),
          topic: context.topic,
          source_id: context.source_id
        )

        Enum.map(messages, &Message.failed(&1, reason))
    end
  end

  @spec produce_to_kafka(
          [Broadway.Message.t()],
          String.t(),
          atom(),
          pos_integer(),
          pos_integer() | nil
        ) ::
          :ok | {:error, term()}
  defp produce_to_kafka(messages, topic, source_token, partitions, backend_id) do
    key = to_string(source_token)

    # Split messages evenly across partitions and fire each chunk concurrently.
    # Events are fetched from ETS and encoded one at a time inside each task so
    # that no full LogEvent list is ever held in memory alongside the encoded form.
    chunk_size = max(1, ceil(length(messages) / partitions))

    messages
    |> Enum.chunk_every(chunk_size)
    |> Enum.with_index()
    |> Task.async_stream(
      fn {chunk, partition} ->
        records =
          Enum.flat_map(chunk, fn %{data: {id, tid}} ->
            case :ets.lookup(tid, id) do
              [{^id, _status, log_event}] ->
                [{key, KafkaSerializer.encode(log_event, backend_id)}]

              [] ->
                []
            end
          end)

        :brod.produce_sync(:logflare_kafka_client, topic, partition, key, records)
      end,
      ordered: false,
      timeout: 10_000
    )
    |> Enum.reduce(:ok, fn
      _result, {:error, _} = err -> err
      {:ok, :ok}, :ok -> :ok
      {:ok, {:error, _} = err}, :ok -> err
      {:exit, reason}, :ok -> {:error, {:task_exit, reason}}
    end)
  end

  defp maybe_requeue_failed([], _config), do: :ok
  defp maybe_requeue_failed(_failed, %{max_retries: 0}), do: :ok
end
