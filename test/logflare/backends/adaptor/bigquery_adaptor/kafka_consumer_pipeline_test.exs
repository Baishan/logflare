defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.KafkaConsumerPipelineTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.KafkaConsumerPipeline
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.KafkaSerializer

  @bq_ok {:ok, %Model.TableDataInsertAllResponse{insertErrors: nil}}

  # Build a Broadway.Message as it arrives from BroadwayKafka:
  #   - data  = raw serialized binary (the Kafka value)
  #   - metadata.key = Kafka partition key (source_token string)
  defp kafka_message(source_token, log_event, backend_id) do
    %Message{
      data: KafkaSerializer.encode(log_event, backend_id),
      metadata: %{key: to_string(source_token)},
      acknowledger: {KafkaConsumerPipeline, :ack_id, :ack_data}
    }
  end

  # Build a Broadway.Message whose data is already the decoded map,
  # i.e. as it looks after handle_message/3 runs (what handle_batch/4 receives).
  defp decoded_message(log_event, backend_id) do
    data = KafkaSerializer.decode(KafkaSerializer.encode(log_event, backend_id))

    %Message{
      data: data,
      metadata: %{key: to_string(log_event.source_uuid)},
      acknowledger: {KafkaConsumerPipeline, :ack_id, :ack_data}
    }
  end

  defp batch_info(source_token, backend_id, size \\ 1) do
    %Broadway.BatchInfo{
      batcher: :bq,
      batch_key: "#{source_token}:#{backend_id || "nil"}:0",
      size: size,
      trigger: :flush
    }
  end

  # -------------------------------------------------------------------------
  # handle_message/3
  # -------------------------------------------------------------------------

  describe "handle_message/3" do
    test "decodes binary payload and sets batcher to :bq" do
      source = build(:source)
      log_event = build(:log_event, source: source)
      message = kafka_message(source.token, log_event, nil)

      result = KafkaConsumerPipeline.handle_message(:default, message, %{})

      assert result.batcher == :bq
      assert is_map(result.data)
      assert result.data["source_id"] == log_event.source_id
    end

    test "sets compound batch_key 'source_token:nil:shard' when no backend_id" do
      source = build(:source)
      log_event = build(:log_event, source: source)
      message = kafka_message(source.token, log_event, nil)

      result = KafkaConsumerPipeline.handle_message(:default, message, %{})

      assert String.starts_with?(result.batch_key, "#{source.token}:nil:")
    end

    test "sets compound batch_key with numeric backend_id and shard suffix" do
      source = build(:source)
      log_event = build(:log_event, source: source)
      message = kafka_message(source.token, log_event, 42)

      result = KafkaConsumerPipeline.handle_message(:default, message, %{})

      assert String.starts_with?(result.batch_key, "#{source.token}:42:")
    end

    test "preserves all event fields in decoded data" do
      source = build(:source)
      log_event = build(:log_event, source: source)
      message = kafka_message(source.token, log_event, nil)

      result = KafkaConsumerPipeline.handle_message(:default, message, %{})

      assert result.data["id"] == log_event.id
      assert result.data["source_id"] == log_event.source_id
    end
  end

  # -------------------------------------------------------------------------
  # handle_batch/4 — default backend (no explicit backend_id)
  # -------------------------------------------------------------------------

  describe "handle_batch/4 with default backend" do
    setup do
      insert(:plan, name: "Free")
      :ok
    end

    test "routes to user's default BQ project and dataset, with correct source_token" do
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

      messages = [decoded_message(log_event, nil)]
      KafkaConsumerPipeline.handle_batch(:bq, messages, batch_info(source.token, nil), %{})

      assert_receive {:bq_write, context}
      assert context.bigquery_project_id == default_backend.config.project_id
      assert context.bigquery_dataset_id == default_backend.config.dataset_id
      # source_token determines which BQ table the event is written to
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

      messages = [decoded_message(log_event, nil)]
      KafkaConsumerPipeline.handle_batch(:bq, messages, batch_info(source.token, nil), %{})

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

      # Each chunk of up to 500 rows becomes one stream_batch! call.
      # With 3 events there is one chunk, so expect is called once.
      Logflare.Google.BigQuery
      |> expect(:stream_batch!, fn context, rows ->
        send(pid, {:bq_write, context, rows})
        @bq_ok
      end)

      messages = Enum.map(log_events, &decoded_message(&1, nil))

      KafkaConsumerPipeline.handle_batch(
        :bq,
        messages,
        batch_info(source.token, nil, 3),
        %{}
      )

      assert_receive {:bq_write, context, rows}
      assert length(rows) == 3
      assert context.source_token == source.token
    end

    test "returns the message list unchanged" do
      user = insert(:user)
      source = insert(:source, user: user)
      log_event = build(:log_event, source: source)

      Logflare.Google.BigQuery
      |> stub(:stream_batch!, fn _context, _rows -> @bq_ok end)

      messages = [decoded_message(log_event, nil)]
      result = KafkaConsumerPipeline.handle_batch(:bq, messages, batch_info(source.token, nil), %{})

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

      messages = [decoded_message(log_event, backend.id)]

      KafkaConsumerPipeline.handle_batch(
        :bq,
        messages,
        batch_info(source.token, backend.id),
        %{}
      )

      assert_receive {:bq_write, context}
      assert context.bigquery_project_id == "backend-project"
      assert context.bigquery_dataset_id == "backend-dataset"
      # source_token still identifies the BQ table within the project
      assert context.source_token == source.token
      assert context.source_id == source.id
    end

    test "routes different sources to different BQ tables via source_token" do
      user = insert(:user)
      source_a = insert(:source, user: user)
      source_b = insert(:source, user: user)

      backend =
        insert(:backend,
          user_id: user.id,
          type: :bigquery,
          config: %{project_id: "shared-project", dataset_id: "shared-dataset"}
        )

      log_event_a = build(:log_event, source: source_a)
      log_event_b = build(:log_event, source: source_b)
      pid = self()

      Logflare.Google.BigQuery
      |> stub(:stream_batch!, fn context, _rows ->
        send(pid, {:bq_write, context.source_token})
        @bq_ok
      end)

      KafkaConsumerPipeline.handle_batch(
        :bq,
        [decoded_message(log_event_a, backend.id)],
        batch_info(source_a.token, backend.id),
        %{}
      )

      KafkaConsumerPipeline.handle_batch(
        :bq,
        [decoded_message(log_event_b, backend.id)],
        batch_info(source_b.token, backend.id),
        %{}
      )

      assert_receive {:bq_write, token_a}
      assert_receive {:bq_write, token_b}
      assert token_a == source_a.token
      assert token_b == source_b.token
      refute token_a == token_b
    end
  end

  # -------------------------------------------------------------------------
  # handle_batch/4 — missing source
  # -------------------------------------------------------------------------

  describe "handle_batch/4 when source is not found" do
    test "drops the batch and logs a warning instead of raising" do
      missing_source_id = 999_999_999
      data = %{"source_id" => missing_source_id, "backend_id" => nil, "id" => "evt-x", "body" => %{}}

      messages = [
        %Message{
          data: data,
          metadata: %{key: "unknown-token"},
          acknowledger: {KafkaConsumerPipeline, :ack_id, :ack_data}
        }
      ]

      batch_info = %Broadway.BatchInfo{
        batcher: :bq,
        batch_key: "unknown-token:nil",
        size: 1,
        trigger: :flush
      }

      Logflare.Google.BigQuery |> reject(:stream_batch!, 2)

      log =
        capture_log(fn ->
          result = KafkaConsumerPipeline.handle_batch(:bq, messages, batch_info, %{})
          assert result == messages
        end)

      assert log =~ "source not found"
    end
  end
end
