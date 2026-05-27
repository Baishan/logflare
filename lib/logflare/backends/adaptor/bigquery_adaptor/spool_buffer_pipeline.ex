defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.SpoolBufferPipeline do
  @moduledoc false

  use Broadway

  require Logger
  require OpenTelemetry.Tracer

  alias Broadway.Message
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Cache, as: BackendsCache
  alias Logflare.SpoolBuffer
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.EventUtils
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.LogEvent, as: LE
  alias Logflare.Sources
  alias Logflare.Sources.Source.BigQuery.Schema
  alias Logflare.Users

  # BQ max is 10MB — stay well under it
  @max_batch_length 6_000_000
  # Broadway accumulates up to this many messages before handle_batch fires.
  # Within handle_batch these are split into @bq_insert_chunk sub-batches
  # and written to BQ in parallel via Task.async_stream.
  @max_batch_size 5_000
  # BQ streaming insert row limit per API call
  @bq_insert_chunk 500

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    source = Keyword.fetch!(args, :source)
    backend = Keyword.get(args, :backend)
    backend_id = backend && backend.id

    Broadway.start_link(__MODULE__,
      name: Keyword.fetch!(args, :name),
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 10],
      producer: [
        module:
          {SpoolBuffer,
           [
             source_id: source.id,
             backend_id: backend_id
           ]},
        transformer: {__MODULE__, :transform, []},
        concurrency: 1
      ],
      processors: [default: [concurrency: 8, max_demand: 100]],
      batchers: [
        bq: [
          concurrency: 8,
          batch_size: bq_batch_size_splitter(),
          batch_timeout: 1_500,
          max_demand: @max_batch_size
        ]
      ],
      context: %{source_id: source.id, backend_id: backend && backend.id}
    )
  end

  @impl Broadway
  def process_name({:via, module, {registry, identifier}}, base_name) do
    {:via, module, {registry, {identifier, base_name}}}
  end

  def process_name(proc_name, base_name) do
    String.to_atom("#{proc_name}-#{base_name}")
  end

  @spec transform(LE.t(), term()) :: Message.t()
  def transform(%LE{} = event, _opts) do
    %Message{
      data: event,
      acknowledger: Broadway.NoopAcknowledger.init()
    }
  end

  @impl Broadway
  def handle_message(_processor, %Message{data: %LE{}} = message, _context) do
    # Events arrive as %LogEvent{} structs — no decoding needed.
    message
    |> Message.put_batcher(:bq)
  end

  def handle_message(_processor, message, _context) do
    Logger.warning("SpoolBufferPipeline: unexpected message data type, skipping",
      data: inspect(message.data)
    )

    Message.failed(message, "unexpected data type")
  end

  @impl Broadway
  def handle_batch(:bq, messages, batch_info, %{source_id: source_id, backend_id: backend_id}) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{backend_type: :bigquery_spool_buffer}
    )

    source = Sources.Cache.get_by_id(source_id)

    if is_nil(source) do
      Logger.warning("SpoolBufferPipeline: source not found, dropping batch",
        source_id: source_id,
        backend_id: backend_id
      )

      messages
    else
      backend = if backend_id, do: BackendsCache.get_backend(backend_id)
      {project_id, dataset_id} = resolve_bq_config(source, backend)

      log_events = Enum.map(messages, fn m -> m.data end)

      maybe_update_schema(source, backend_id, log_events)

      bq_context = %{
        bigquery_project_id: project_id,
        bigquery_dataset_id: dataset_id,
        source_token: source.token,
        source_id: source.id,
        user_id: source.user_id,
        system_source: source.system_source
      }

      OpenTelemetry.Tracer.with_span "ingest.bigquery_batch", %{
        attributes: %{
          source_id: source.id,
          source_token: source.token,
          backend_id: backend_id,
          ingest_batch_size: batch_info.size
        }
      } do
        if source.bq_storage_write_api do
          BigQueryAdaptor.insert_log_events_via_storage_write_api(log_events,
            project_id: project_id,
            dataset_id: dataset_id,
            source_token: source.token,
            source_id: source.id,
            backend_id: backend_id
          )
        else
          log_events
          |> Enum.map(&le_to_bq_row/1)
          |> Enum.chunk_every(@bq_insert_chunk)
          |> Task.async_stream(
            fn chunk -> stream_batch(bq_context, chunk) end,
            ordered: false,
            timeout: 30_000
          )
          |> Stream.run()
        end
      end

      messages
    end
  end

  # --- Private helpers ---

  @spec resolve_bq_config(Sources.Source.t(), term()) :: {String.t(), String.t()}
  defp resolve_bq_config(_source, %{config: %{project_id: pid, dataset_id: did}})
       when is_binary(pid) and is_binary(did) do
    {pid, did}
  end

  defp resolve_bq_config(source, _backend) do
    user = Users.Cache.get(source.user_id)
    default_backend = Backends.get_default_backend(user)
    {default_backend.config.project_id, default_backend.config.dataset_id}
  end

  @spec maybe_update_schema(Sources.Source.t(), pos_integer() | nil, [LE.t()]) :: :ok
  defp maybe_update_schema(source, backend_id, log_events) do
    schema_name = Backends.via_source(source, Schema, backend_id)

    case GenServer.whereis(schema_name) do
      nil ->
        :ok

      pid ->
        for log_event <- log_events do
          Schema.update(pid, log_event, source)
        end

        :ok
    end
  end

  defp stream_batch(context, rows) do
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

  @spec bq_batch_size_splitter() ::
          {{non_neg_integer(), non_neg_integer()}, (Broadway.Message.t(), term() -> term())}
  def bq_batch_size_splitter do
    {
      {@max_batch_size, @max_batch_length},
      fn
        _message, {1, _len} ->
          {:emit, {@max_batch_size, @max_batch_length}}

        message, {count, len} ->
          length = :erlang.external_size(message.data)

          if len - length <= 0 do
            {:emit, {@max_batch_size, @max_batch_length}}
          else
            {:cont, {count - 1, len - length}}
          end
      end
    }
  end
end
