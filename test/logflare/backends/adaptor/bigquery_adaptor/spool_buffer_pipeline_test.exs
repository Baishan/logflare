defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.SpoolBufferPipelineTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.SpoolBufferPipeline
  alias Logflare.LogEvent, as: LE

  @bq_ok {:ok, %Model.TableDataInsertAllResponse{insertErrors: nil}}

  # Build a Broadway.Message as it looks after transform/2 runs —
  # data is a %LogEvent{} struct, acknowledger is NoopAcknowledger.
  defp bq_message(%LE{} = log_event) do
    SpoolBufferPipeline.transform(log_event, [])
  end

  defp batch_info(size \\ 1) do
    %Broadway.BatchInfo{
      batcher: :bq,
      batch_key: "default",
      size: size,
      trigger: :flush
    }
  end

  # -------------------------------------------------------------------------
  # transform/2
  # -------------------------------------------------------------------------

  describe "transform/2" do
    test "wraps a LogEvent in a Broadway.Message with NoopAcknowledger" do
      source = build(:source)
      log_event = build(:log_event, source: source)

      message = SpoolBufferPipeline.transform(log_event, [])

      assert %Message{} = message
      assert message.data == log_event
      assert message.acknowledger == Broadway.NoopAcknowledger.init()
    end
  end

  # -------------------------------------------------------------------------
  # handle_batch/4 — default backend
  # -------------------------------------------------------------------------

  describe "handle_batch/4 with default backend" do
    setup do
      insert(:plan, name: "Free")
      :ok
    end

    test "routes to user's default BQ project and dataset" do
      user = insert(:user)
      source = insert(:source, user: user)
      log_event = build(:log_event, source: source)

      default_backend = Backends.get_default_backend(user)
      pid = self()

      Logflare.Google.BigQuery
      |> expect(:stream_batch!, fn context, _rows ->
        send(pid, {:bq_write, context})
        @bq_ok
      end)

      messages = [bq_message(log_event)]
      context = %{source_id: source.id, backend_id: nil}

      SpoolBufferPipeline.handle_batch(:bq, messages, batch_info(), context)

      assert_receive {:bq_write, context}
      assert context.bigquery_project_id == default_backend.config.project_id
      assert context.bigquery_dataset_id == default_backend.config.dataset_id
      assert context.source_token == source.token
      assert context.source_id == source.id
      assert context.user_id == user.id
    end

    test "uses custom bigquery_project_id when set on user" do
      user = insert(:user, bigquery_project_id: "custom-project", bigquery_dataset_id: "custom-ds")
      source = insert(:source, user: user)
      log_event = build(:log_event, source: source)
      pid = self()

      Logflare.Google.BigQuery
      |> expect(:stream_batch!, fn context, _rows ->
        send(pid, {:bq_write, context})
        @bq_ok
      end)

      messages = [bq_message(log_event)]
      context = %{source_id: source.id, backend_id: nil}

      SpoolBufferPipeline.handle_batch(:bq, messages, batch_info(), context)

      assert_receive {:bq_write, context}
      assert context.bigquery_project_id == "custom-project"
      assert context.bigquery_dataset_id == "custom-ds"
      assert context.source_token == source.token
    end

    test "sends all events in batch to BigQuery" do
      user = insert(:user)
      source = insert(:source, user: user)
      log_events = Enum.map(1..3, fn _ -> build(:log_event, source: source) end)
      pid = self()

      Logflare.Google.BigQuery
      |> expect(:stream_batch!, fn context, rows ->
        send(pid, {:bq_write, context, rows})
        @bq_ok
      end)

      messages = Enum.map(log_events, &bq_message/1)
      context = %{source_id: source.id, backend_id: nil}

      SpoolBufferPipeline.handle_batch(:bq, messages, batch_info(3), context)

      assert_receive {:bq_write, _context, rows}
      assert length(rows) == 3
    end

    test "returns the message list unchanged" do
      user = insert(:user)
      source = insert(:source, user: user)
      log_event = build(:log_event, source: source)

      Logflare.Google.BigQuery
      |> stub(:stream_batch!, fn _context, _rows -> @bq_ok end)

      messages = [bq_message(log_event)]
      context = %{source_id: source.id, backend_id: nil}

      result = SpoolBufferPipeline.handle_batch(:bq, messages, batch_info(), context)

      assert result == messages
    end
  end

  # -------------------------------------------------------------------------
  # handle_batch/4 — explicit backend
  # -------------------------------------------------------------------------

  describe "handle_batch/4 with explicit backend" do
    setup do
      insert(:plan, name: "Free")
      :ok
    end

    test "uses backend config project_id and dataset_id instead of user defaults" do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          user_id: user.id,
          type: :bigquery,
          config: %{project_id: "backend-project", dataset_id: "backend-dataset"}
        )

      log_event = build(:log_event, source: source)
      pid = self()

      Logflare.Google.BigQuery
      |> expect(:stream_batch!, fn context, _rows ->
        send(pid, {:bq_write, context})
        @bq_ok
      end)

      messages = [bq_message(log_event)]
      context = %{source_id: source.id, backend_id: backend.id}

      SpoolBufferPipeline.handle_batch(:bq, messages, batch_info(), context)

      assert_receive {:bq_write, context}
      assert context.bigquery_project_id == "backend-project"
      assert context.bigquery_dataset_id == "backend-dataset"
      assert context.source_token == source.token
      assert context.source_id == source.id
    end
  end

  # -------------------------------------------------------------------------
  # handle_batch/4 — missing source
  # -------------------------------------------------------------------------

  describe "handle_batch/4 when source is not found" do
    test "drops the batch and logs a warning instead of raising" do
      source = build(:source, id: 999_999_999)
      log_event = build(:log_event, source: source)

      Logflare.Google.BigQuery |> reject(:stream_batch!, 2)

      messages = [bq_message(log_event)]
      context = %{source_id: 999_999_999, backend_id: nil}

      log =
        capture_log(fn ->
          result = SpoolBufferPipeline.handle_batch(:bq, messages, batch_info(), context)
          assert result == messages
        end)

      assert log =~ "source not found"
    end
  end
end
