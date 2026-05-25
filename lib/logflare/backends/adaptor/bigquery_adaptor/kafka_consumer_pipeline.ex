defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.KafkaConsumerPipeline do
  @moduledoc false

  use Broadway

  require Logger
  require OpenTelemetry.Tracer

  alias Broadway.Message
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.KafkaSerializer
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.EventUtils
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.LogEvent, as: LE
  alias Logflare.Sources

  # BQ max is 10MB
  @max_batch_length 6_000_000
  @max_batch_size 500

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    {name, args} = Keyword.pop!(args, :name)
    source = Keyword.fetch!(args, :source)
    backend = Keyword.fetch!(args, :backend)
    project_id = Keyword.fetch!(args, :bigquery_project_id)
    dataset_id = Keyword.fetch!(args, :bigquery_dataset_id)

    kafka_config = Application.get_env(:logflare, :kafka, [])
    hosts = Keyword.fetch!(kafka_config, :hosts)
    topic = Keyword.fetch!(kafka_config, :topic)
    group_id = "logflare_bq_#{source.id}_#{backend.id}"

    opts = [
      name: name,
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 10],
      producer: [
        module:
          {BroadwayKafka.Producer,
           [
             hosts: hosts,
             group_id: group_id,
             topics: [topic],
             client_config: []
           ]},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 8, max_demand: 100]
      ],
      batchers: [
        bq: [
          concurrency: 16,
          batch_size: bq_batch_size_splitter(),
          batch_timeout: 1_500,
          max_demand: @max_batch_size
        ]
      ],
      context: %{
        bigquery_project_id: project_id,
        bigquery_dataset_id: dataset_id,
        source_token: source.token,
        bq_storage_write_api: source.bq_storage_write_api,
        source_id: source.id,
        backend_id: backend.id,
        user_id: source.user_id,
        system_source: source.system_source
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

  @impl Broadway
  def handle_message(_processor, message, context) do
    log_event = deserialize(message.data.value, context)

    message
    |> Message.update_data(fn _ -> log_event end)
    |> Message.put_batcher(:bq)
  end

  @impl Broadway
  def handle_batch(:bq, messages, batch_info, context) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{backend_type: :bigquery_kafka}
    )

    attributes =
      for {k, v} <- [
            source_id: context.source_id,
            source_token: context.source_token,
            backend_id: context.backend_id,
            ingest_batch_size: batch_info.size,
            ingest_batch_trigger: batch_info.trigger
          ],
          v != nil,
          do: {k, v}

    OpenTelemetry.Tracer.with_span "ingest.bigquery_batch", %{
      attributes: Map.new(attributes)
    } do
      source = Sources.Cache.get_by_id(context.source_id)

      if source && source.bq_storage_write_api do
        log_events = Enum.map(messages, & &1.data)

        OpenTelemetry.Tracer.with_span "ingest.bq_insert", %{
          attributes: %{insert_method: :bq_storage_write}
        } do
          BigQueryAdaptor.insert_log_events_via_storage_write_api(log_events,
            project_id: context.bigquery_project_id,
            dataset_id: context.bigquery_dataset_id,
            source_id: context.source_id,
            source_token: context.source_token,
            backend_id: context.backend_id
          )
        end
      else
        OpenTelemetry.Tracer.with_span "ingest.bq_insert", %{
          attributes: %{insert_method: :bq_streaming_insert}
        } do
          stream_batch(context, messages)
        end
      end
    end

    messages
  end

  defp deserialize(value, context) do
    data = KafkaSerializer.decode(value)

    %LE{
      id: data["id"],
      body: data["body"],
      source_id: context.source_id,
      source_uuid: context.source_token,
      is_popped: true
    }
  end

  defp stream_batch(context, messages) do
    rows = Enum.map(messages, &le_to_bq_row(&1.data))

    case BigQuery.stream_batch!(context, rows) do
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}} ->
        :ok

      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: errors}} ->
        Logger.warning("BigQuery insert errors", errors: inspect(errors))

      {:error, %Tesla.Env{} = response} ->
        Logger.warning("BigQuery stream batch error",
          error: GenUtils.get_tesla_error_message(response)
        )

      {:error, response} ->
        Logger.warning("BigQuery stream batch unknown error", error: inspect(response))
    end
  end

  defp le_to_bq_row(%LE{body: body, id: id}) do
    {:ok, bq_timestamp} = DateTime.from_unix(body["timestamp"], :microsecond)

    body =
      for {k, v} <- body, into: %{} do
        if is_map(v) do
          {k, EventUtils.prepare_for_ingest(v)}
        else
          {k, v}
        end
      end
      |> Map.put("timestamp", bq_timestamp)
      |> Map.put("event_message", body["event_message"])

    %Model.TableDataInsertAllRequestRows{insertId: id, json: body}
  end

  def bq_batch_size_splitter do
    {
      {@max_batch_size, @max_batch_length},
      fn
        _message, {1, _len} ->
          {:emit, {@max_batch_size, @max_batch_length}}

        message, {count, len} ->
          length = :erlang.external_size(message.data.body)

          if len - length <= 0 do
            {:emit, {@max_batch_size, @max_batch_length}}
          else
            {:cont, {count - 1, len - length}}
          end
      end
    }
  end
end
