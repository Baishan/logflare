defmodule Logflare.Sources.Source.BigQuery.Pipeline do
  @moduledoc false
  use Broadway

  require Logger

  alias Broadway.Message
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.AccountEmail
  alias Logflare.Backends
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.EventUtils
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.LogEvent, as: LE
  alias Logflare.Mailer
  alias Logflare.Sources
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.BufferProducer
  alias Logflare.Sources.Source.BigQuery.Schema
  alias Logflare.Sources.Source.Supervisor
  alias Logflare.Sources
  alias Logflare.Users
  alias Logflare.PubSubRates
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Utils
  require OpenTelemetry.Tracer

  @behaviour Broadway.Acknowledger

  # BQ max is 10MB
  # https://cloud.google.com/bigquery/quotas#streaming_inserts
  @max_batch_length 6_000_000
  @max_batch_size 500
  @max_retries 0

  def start_link(args, opts \\ []) do
    {name, args} = Keyword.pop(args, :name)
    source = Keyword.get(args, :source)
    backend = Keyword.get(args, :backend)

    max_retries =
      Application.get_env(:logflare, :bigquery_pipeline, [])
      |> Keyword.get(:max_retries, @max_retries)

    ack_config = %{max_retries: max_retries}

    opts =
      Keyword.merge(
        [
          # top-level will apply to all children
          name: name,
          hibernate_after: 5_000,
          spawn_opt: [
            fullsweep_after: 10
          ],
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
                 ref: {{source.id, backend.id, args[:pipeline_ref]}, ack_config}
               ]}
          ],
          processors: [
            default: [concurrency: 8, max_demand: 100]
          ],
          batchers: [
            bq: [
              concurrency: 16,
              batch_size: bq_batch_size_splitter(),
              batch_timeout: 1_500,
              # must be set when using custom batch_size splitter
              max_demand: @max_batch_size
            ]
          ],
          context: %{
            bigquery_project_id: args[:bigquery_project_id],
            bigquery_dataset_id: args[:bigquery_dataset_id],
            source_token: source.token,
            bq_storage_write_api: source.bq_storage_write_api,
            source_id: source.id,
            backend_id: Map.get(backend || %{}, :id),
            user_id: source.user_id,
            system_source: source.system_source
          }
        ],
        opts
      )

    Broadway.start_link(
      __MODULE__,
      opts
    )
  end

  # pipeline name is sharded
  @impl Broadway
  def process_name({:via, module, {registry, identifier}}, base_name) do
    {:via, module, {registry, {identifier, base_name}}}
  end

  def process_name(proc_name, base_name) do
    String.to_atom("#{proc_name}-#{base_name}")
  end

  # Broadway transformer for custom producer
  def transform(event, args) do
    ref = args[:ref]

    %Message{
      data: event,
      acknowledger: {__MODULE__, ref, :ack_data}
    }
  end

  @impl Broadway.Acknowledger
  def ack({queue, config}, successful, failed) do
    {sid, bid, _pipeline_ref} = queue

    maybe_requeue_failed({sid, bid}, failed, config)

    backend_metadata =
      if bid do
        Backends.Cache.get_backend(bid).metadata || %{}
      else
        %{}
      end

    case Sources.Cache.get_by_id(sid) do
      nil ->
        Logger.warning("Source not found for ack!", source_id: sid)

        for %{data: {id, tid}} <- successful do
          :ets.delete(tid, id)
        end

      source ->
        metrics = Sources.get_source_metrics_for_ingest(source.token)

        for %{data: {id, tid}} <- successful do
          case :ets.lookup(tid, id) do
            [{^id, _status, le}] -> emit_event_telemetry(queue, source, le, backend_metadata)
            [] -> :ok
          end

          if metrics.avg > 100 do
            :ets.delete(tid, id)
          else
            :ets.update_element(tid, id, {2, :ingested})
          end
        end
    end

    :ok
  end

  @impl Broadway
  def handle_message(_processor_name, message, context) do
    Logger.metadata(
      source_id: context.source_token,
      source_token: context.source_token,
      user_id: context.user_id,
      system_source: context.system_source
    )

    Message.put_batcher(message, :bq)
  end

  @impl Broadway
  def handle_batch(:bq, messages, batch_info, context) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{
        backend_type: :bigquery
      }
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
        # Storage write API needs full LogEvent structs for the adaptor.
        log_events = fetch_events_from_messages(messages)
        maybe_update_schema(log_events, source, context)
        batch_attrs = compute_batch_attrs(log_events, :bq_storage_write)

        OpenTelemetry.Tracer.with_span "ingest.bq_insert", %{attributes: batch_attrs} do
          BigQueryAdaptor.insert_log_events_via_storage_write_api(log_events,
            project_id: context.bigquery_project_id,
            dataset_id: context.bigquery_dataset_id,
            source_id: context.source_id,
            source_token: context.source_token,
            backend_id: context.backend_id
          )
        end
      else
        # Streaming insert path: one-pass ETS fetch + serialization.
        # LogEvents are never accumulated into a list in the batcher heap.
        stream_batch_from_messages(context, messages, source)
      end

      messages
    end
  end

  def le_messages_to_bq_rows(messages) do
    Enum.map(messages, fn message ->
      le_to_bq_row(message.data)
    end)
  end

  @spec le_list_to_bq_rows([LE.t()]) :: [Model.TableDataInsertAllRequestRows.t()]
  def le_list_to_bq_rows(log_events) do
    Enum.map(log_events, &le_to_bq_row/1)
  end

  @spec fetch_events_from_messages([Broadway.Message.t()]) :: [LE.t()]
  defp fetch_events_from_messages(messages) do
    Enum.flat_map(messages, fn
      %{data: {id, tid}} ->
        case :ets.lookup(tid, id) do
          [{^id, _status, log_event}] -> [log_event]
          [] -> []
        end

      %{data: %LE{} = log_event} ->
        [log_event]
    end)
  end

  def le_to_bq_row(%LE{body: body, id: id}) do
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
      |> case do
        %{"start_time" => start_time, "end_time" => end_time} = data
        when is_map_key(data, "resource") and is_map_key(data, "scope") ->
          # round to microseconds
          %{
            data
            | "start_time" => DateTime.from_unix!(start_time, :nanosecond),
              "end_time" => DateTime.from_unix!(end_time, :nanosecond)
          }

        %{"start_time" => start_time} = data
        when is_map_key(data, "resource") and is_map_key(data, "scope") ->
          # round to microseconds
          %{data | "start_time" => DateTime.from_unix!(start_time, :nanosecond)}

        %{"end_time" => end_time} = data
        when is_map_key(data, "resource") and is_map_key(data, "scope") ->
          # round to microseconds
          %{data | "end_time" => DateTime.from_unix!(end_time, :nanosecond)}

        data ->
          data
      end

    %Model.TableDataInsertAllRequestRows{
      insertId: id,
      json: body
    }
  end

  # Public interface kept for backward compatibility and direct test usage.
  # The production streaming path goes through stream_batch_from_messages/3 instead.
  def stream_batch(
        %{source_token: source_token, user_id: user_id, system_source: system_source} = context,
        log_events
      ) do
    Logger.metadata(
      source_id: source_token,
      source_token: source_token,
      user_id: user_id,
      system_source: system_source
    )

    :telemetry.span(
      [:logflare, :ingest, :pipeline, :stream_batch],
      %{source_token: source_token},
      fn -> execute_bigquery_stream_batch(context, log_events) end
    )
  end

  defp execute_bigquery_stream_batch(%{source_token: source_token} = context, log_events) do
    rows = le_list_to_bq_rows(log_events)

    # TODO ... Send some errors through the pipeline again. The generic "retry" error specifically.
    # All others send to the rejected list with the message from BigQuery.
    # See todo in `process_data` also.
    OpenTelemetry.Tracer.with_span "ingest.bq_api_call", %{
      attributes: %{insert_method: :bq_streaming_insert}
    } do
      case BigQuery.stream_batch!(context, rows) do
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}} ->
          OpenTelemetry.Tracer.set_attribute(:insert_error_count, 0)
          :ok

        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: errors}} ->
          OpenTelemetry.Tracer.set_attribute(:insert_error_count, length(errors))
          error_string = inspect(errors)
          OpenTelemetry.Tracer.set_status(:error, error_string)
          Logger.warning("BigQuery insert errors.", error_string: error_string)

        {:error, %Tesla.Env{} = response} ->
          message = GenUtils.get_tesla_error_message(response)
          OpenTelemetry.Tracer.set_status(:error, message)

          case message do
            "Access Denied: BigQuery BigQuery: Streaming insert is not allowed in the free tier" =
                message ->
              disconnect_backend_and_email(source_token, message)

            "The project" <> _tail = message ->
              # "The project web-wtc-1537199112807 has not enabled BigQuery."
              disconnect_backend_and_email(source_token, message)

            _message ->
              Logger.warning("Stream batch response error!",
                tesla_response: GenUtils.get_tesla_error_message(response)
              )
          end

        {:error, response} ->
          OpenTelemetry.Tracer.set_status(:error, inspect(response))
          Logger.warning("Stream batch unknown error!", tesla_response: inspect(response))
      end
    end

    {:ok, %{}}
  end

  # Optimised production path for the streaming insert. Takes Broadway messages
  # directly so that LogEvents are never accumulated into an intermediate list.
  # Each event is fetched from ETS, serialised to a BQ row, and released within
  # a single reduce iteration — peak batcher-heap cost is O(1) events, not O(n).
  defp stream_batch_from_messages(
         %{source_token: source_token, user_id: user_id, system_source: system_source} = context,
         messages,
         source
       ) do
    Logger.metadata(
      source_id: source_token,
      source_token: source_token,
      user_id: user_id,
      system_source: system_source
    )

    :telemetry.span(
      [:logflare, :ingest, :pipeline, :stream_batch],
      %{source_token: source_token},
      fn -> execute_streaming_insert(context, messages, source) end
    )
  end

  @spec execute_streaming_insert(map(), [Broadway.Message.t()], Sources.Source.t() | nil) ::
          {:ok, map()}
  defp execute_streaming_insert(%{source_token: source_token} = context, messages, source) do
    # Compute schema-update config once per batch rather than per event.
    {schema_via, probability} = schema_update_config(source, context)

    # Single pass: ETS fetch → optional schema update → BQ row serialisation.
    # `log_event` is a local binding that goes out of scope after each iteration;
    # the accumulator only holds the growing `rows` list and scalar counters.
    {rows_rev, event_count, batch_bytes} =
      Enum.reduce(messages, {[], 0, 0}, fn message, {rows_acc, count_acc, bytes_acc} ->
        case fetch_event(message) do
          nil ->
            {rows_acc, count_acc, bytes_acc}

          log_event ->
            if schema_via != nil and :rand.uniform() <= probability do
              :ok = Schema.update(schema_via, log_event, source)
            end

            row = le_to_bq_row(log_event)
            bytes = :erlang.external_size(log_event.body)
            {[row | rows_acc], count_acc + 1, bytes_acc + bytes}
        end
      end)

    rows = Enum.reverse(rows_rev)

    OpenTelemetry.Tracer.with_span "ingest.bq_api_call", %{
      attributes: %{
        insert_method: :bq_streaming_insert,
        batch_event_count: event_count,
        batch_bytes: batch_bytes
      }
    } do
      case BigQuery.stream_batch!(context, rows) do
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}} ->
          OpenTelemetry.Tracer.set_attribute(:insert_error_count, 0)
          :ok

        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: errors}} ->
          OpenTelemetry.Tracer.set_attribute(:insert_error_count, length(errors))
          error_string = inspect(errors)
          OpenTelemetry.Tracer.set_status(:error, error_string)
          Logger.warning("BigQuery insert errors.", error_string: error_string)

        {:error, %Tesla.Env{} = response} ->
          message = GenUtils.get_tesla_error_message(response)
          OpenTelemetry.Tracer.set_status(:error, message)

          case message do
            "Access Denied: BigQuery BigQuery: Streaming insert is not allowed in the free tier" =
                message ->
              disconnect_backend_and_email(source_token, message)

            "The project" <> _tail = message ->
              disconnect_backend_and_email(source_token, message)

            _message ->
              Logger.warning("Stream batch response error!",
                tesla_response: GenUtils.get_tesla_error_message(response)
              )
          end

        {:error, response} ->
          OpenTelemetry.Tracer.set_status(:error, inspect(response))
          Logger.warning("Stream batch unknown error!", tesla_response: inspect(response))
      end
    end

    {:ok, %{}}
  end

  @spec fetch_event(Broadway.Message.t()) :: LE.t() | nil
  defp fetch_event(%{data: {id, tid}}) do
    case :ets.lookup(tid, id) do
      [{^id, _status, log_event}] -> log_event
      [] -> nil
    end
  end

  defp fetch_event(%{data: %LE{} = log_event}), do: log_event

  # Computes schema-update probability and via-tuple once per batch.
  # Returns {nil, 0.0} when schema updates should be skipped entirely.
  @spec schema_update_config(Sources.Source.t() | nil, map()) :: {term() | nil, float()}
  defp schema_update_config(nil, _context), do: {nil, 0.0}
  defp schema_update_config(%{lock_schema: true}, _context), do: {nil, 0.0}

  defp schema_update_config(source, context) do
    probability =
      case PubSubRates.Cache.get_local_rates(source.token) do
        %{average_rate: avg} when avg > 0 ->
          min(1.0, max(0.00001, 1.0 / avg))

        _ ->
          1.0
      end

    schema_via = Backends.via_source(source, {Schema, Map.get(context, :backend_id)})
    {schema_via, probability}
  end

  # Used by the storage-write-API path which materialises log_events up front.
  @spec maybe_update_schema([LE.t()], Sources.Source.t() | nil, map()) :: :ok
  defp maybe_update_schema(_log_events, nil, _context), do: :ok
  defp maybe_update_schema(_log_events, %{lock_schema: true}, _context), do: :ok

  defp maybe_update_schema(log_events, source, context) do
    {schema_via, probability} = schema_update_config(source, context)

    for log_event <- log_events, :rand.uniform() <= probability do
      :ok = Schema.update(schema_via, log_event, source)
    end

    :ok
  end

  def process_data(%LE{source_id: source_id} = log_event, context) do
    source = Sources.Cache.get_by_id(source_id)

    # TODO ... We use `ignoreUnknownValues: true` when we do `stream_batch!`. If we set that to `true`
    # then this makes BigQuery check the payloads for new fields. In the response we'll get a list of events that
    # didn't validate.
    # Send those events through the pipeline again, but run them through our schema process this time. Do all
    # these things a max of like 5 times and after that send them to the rejected pile.

    # random sample if local ingest rate is above a certain level
    # dynamic calculation maintains ~1 schema update per second across all rate levels
    if source && not source.lock_schema do
      probability =
        case PubSubRates.Cache.get_local_rates(source.token) do
          %{average_rate: avg} when avg > 0 ->
            min(1.0, max(0.00001, 1.0 / avg))

          _ ->
            1.0
        end

      if :rand.uniform() <= probability do
        :ok =
          Backends.via_source(source, {Schema, Map.get(context, :backend_id)})
          |> Schema.update(log_event, source)
      end
    end

    log_event
  end

  def name(source_id) when is_atom(source_id) do
    String.to_atom("#{source_id}" <> "-pipeline")
  end

  @spec compute_batch_attrs([LE.t()], atom()) :: map()
  defp compute_batch_attrs(log_events, bq_api_tag) do
    event_count = length(log_events)
    bytes = log_events |> Enum.map(&:erlang.external_size(&1.body)) |> Enum.sum()

    %{insert_method: bq_api_tag, batch_event_count: event_count, batch_bytes: bytes}
  end

  defp disconnect_backend_and_email(source_id, message) when is_atom(source_id) do
    source = Sources.Cache.get_by(token: source_id)
    user = Users.Cache.get(source.user_id)

    defaults = %{
      bigquery_dataset_location: nil,
      bigquery_project_id: nil,
      bigquery_dataset_id: nil,
      bigquery_processed_bytes_limit: 10_000_000_000
    }

    Logger.warning("user audit: BigQuery backend auto-disconnect triggered",
      action: "user.bq_auto_disconnect",
      user_id: user.id,
      user_email: user.email,
      source_token: source_id,
      reason: message
    )

    case Users.update_user_allowed(user, defaults) do
      {:ok, user} ->
        Supervisor.reset_all_user_sources(user)

        user
        |> AccountEmail.backend_disconnected(message)
        |> Mailer.deliver()

        Logger.warning("user audit: BigQuery backend auto-disconnected",
          action: "user.bq_auto_disconnected",
          user_id: user.id,
          user_email: user.email,
          source_token: source_id,
          reason: message
        )

      {:error, changeset} ->
        Logger.error("user audit: BigQuery backend auto-disconnect failed",
          action: "user.bq_auto_disconnect_failed",
          user_id: user.id,
          user_email: user.email,
          source_token: source_id,
          reason: message,
          errors: inspect(changeset.errors)
        )
    end
  end

  # Requeue failed events if the number of previous retries is less than @max_retries
  defp maybe_requeue_failed(_, [], _), do: :ok
  defp maybe_requeue_failed(_, _, %{max_retries: 0}), do: :ok

  defp maybe_requeue_failed({_sid, _bid} = sid_bid, failed, %{max_retries: max_retries}) do
    events =
      Enum.flat_map(failed, fn %{data: {id, tid}} ->
        case :ets.lookup(tid, id) do
          [{^id, _status, %LE{retries: retries} = le}] when retries < max_retries ->
            [%LE{le | retries: (retries || 0) + 1}]

          _ ->
            []
        end
      end)

    requeue(sid_bid, events)
  end

  defp requeue(_, []), do: :ok

  defp requeue(sid_bid, events) do
    Logger.info("Requeuing #{length(events)} BigQuery events for retry")

    IngestEventQueue.delete_batch(sid_bid, events)
    IngestEventQueue.add_to_table(sid_bid, events)
  end

  defp emit_event_telemetry({sid, bid, _}, source, le, backend_metadata) do
    # emit telemetry on event
    event_labels = Sources.get_labels_from_event(source, le)

    metrics = %{ingested_bytes: :erlang.external_size(le.body)}

    metadata =
      %{
        "source_id" => sid,
        "backend_id" => bid,
        "source_uuid" => Utils.stringify(source.token),
        "user_id" => source.user_id,
        "system_source" => source.system_source
      }
      |> Map.merge(event_labels)
      |> Map.merge(backend_metadata)

    :telemetry.execute([:logflare, :backends, :ingest], metrics, metadata)
  end

  # https://hexdocs.pm/broadway/Broadway.html#start_link/2
  # split batch sizes based on json size
  # ensure that we are well below the 10MB limit
  def bq_batch_size_splitter do
    {
      {@max_batch_size, @max_batch_length},
      fn
        # reach max count, emit
        _message, {1, _len} ->
          {:emit, {@max_batch_size, @max_batch_length}}

        # check content length
        message, {count, len} ->
          length = message_size(message.data)

          if len - length <= 0 do
            # below max batch count, but reach max batch length
            {:emit, {@max_batch_size, @max_batch_length}}
          else
            # below max batch count, below max batch length
            {:cont, {count - 1, len - length}}
          end
      end
    }
  end

  # For {id, tid} pointers the actual payload size is unknown until ETS lookup at batch time.
  # Return 0 so the count limit (@max_batch_size) governs batch splits instead.
  def message_size({_id, _tid}), do: 0
  def message_size(%LE{body: body}), do: :erlang.external_size(body)
  def message_size(data), do: :erlang.external_size(data)
end
