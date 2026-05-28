defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.S3Pipeline do
  @moduledoc false

  use Broadway

  import Bitwise

  require Logger

  alias Broadway.Message
  alias Logflare.Backends.BufferProducer

  @behaviour Broadway.Acknowledger

  @max_batch_size 50_000
  @default_batch_timeout 5_000
  # S3 multipart minimum part size (except the final part)
  @multipart_chunk_size 5 * 1024 * 1024

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(args) do
    {name, args} = Keyword.pop!(args, :name)
    source = Keyword.fetch!(args, :source)
    backend = Keyword.fetch!(args, :backend)

    s3_config = Application.get_env(:logflare, :s3_pipeline, [])
    bucket = Keyword.fetch!(s3_config, :bucket)
    partitions = Keyword.get(s3_config, :partitions, 4)
    batch_timeout = Keyword.get(s3_config, :batch_timeout, @default_batch_timeout)
    upload_mode = Keyword.get(s3_config, :upload_mode, :put_object)

    Broadway.start_link(__MODULE__,
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
             ref: {{source.id, backend.id, nil}, %{}}
           ]}
      ],
      processors: [default: [concurrency: 8, max_demand: 1_000]],
      batchers: [
        s3: [
          concurrency: partitions,
          batch_size: @max_batch_size,
          batch_timeout: batch_timeout
        ]
      ],
      context: %{
        bucket: bucket,
        partitions: partitions,
        source_id: source.id,
        upload_mode: upload_mode
      }
    )
  end

  @impl Broadway
  def process_name({:via, module, {registry, identifier}}, base_name) do
    {:via, module, {registry, {identifier, base_name}}}
  end

  def process_name(proc_name, base_name) do
    String.to_atom("#{proc_name}-#{base_name}")
  end

  @spec transform(term(), keyword()) :: Message.t()
  def transform(event, args) do
    %Message{
      data: event,
      acknowledger: {__MODULE__, args[:ref], :ack_data}
    }
  end

  @impl Broadway.Acknowledger
  def ack(_ref, successful, failed) do
    for %{data: {id, tid}} <- successful ++ failed do
      :ets.delete(tid, id)
    end

    :ok
  end

  @impl Broadway
  def handle_message(_processor, message, _context) do
    Message.put_batcher(message, :s3)
  end

  @impl Broadway
  def handle_batch(:s3, messages, batch_info, %{
        bucket: bucket,
        source_id: source_id,
        partitions: partitions,
        upload_mode: upload_mode
      }) do
    dbg("Handle batch #{Enum.count(messages)}")
    dbg(batch_info)

    partition = :rand.uniform(partitions) - 1
    file_key = "#{partition}/#{generate_uuidv7()}.ndjson.gz"

    result =
      case upload_mode do
        :gzip -> upload_gzip(messages, bucket, file_key)
        :put_object -> upload_put_object(messages, bucket, file_key)
        :multipart -> upload_multipart(messages, bucket, file_key)
      end

    case result do
      {:ok, _} ->
        :telemetry.execute(
          [:logflare, :backends, :pipeline, :handle_batch],
          %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
          %{backend_type: :bigquery_s3_pipeline}
        )

        Logger.debug("s3_pipeline: wrote #{batch_info.size} events to s3",
          source_id: source_id,
          key: file_key
        )

        messages

      {:error, reason} ->
        Logger.error("s3_pipeline: S3 write failed",
          source_id: source_id,
          key: file_key,
          error: inspect(reason)
        )

        Enum.map(messages, &Message.failed(&1, reason))
    end
  end

  # Original approach: build full uncompressed iodata then one-shot gzip.
  # Peak memory = uncompressed_iodata + compressed_binary simultaneously.
  defp upload_gzip(messages, bucket, file_key) do
    body =
      Enum.flat_map(messages, fn %{data: {id, tid}} ->
        case :ets.lookup(tid, id) do
          [{^id, _status, log_event}] -> [Jason.encode!(log_event.body), "\n"]
          [] -> []
        end
      end)
      |> :zlib.gzip()

    ExAws.S3.put_object(bucket, file_key, body,
      headers: %{"content-type" => "application/x-ndjson", "content-encoding" => "gzip"}
    )
    |> ExAws.request()
  end

  defp upload_put_object(messages, bucket, file_key) do
    body = compress_to_binary(messages)

    ExAws.S3.put_object(bucket, file_key, body,
      headers: %{"content-type" => "application/x-ndjson", "content-encoding" => "gzip"}
    )
    |> ExAws.request()
  end

  defp upload_multipart(messages, bucket, file_key) do
    multipart_stream(messages)
    |> ExAws.S3.upload(bucket, file_key,
      content_type: "application/x-ndjson",
      content_encoding: "gzip"
    )
    |> ExAws.request()
  end

  # Compress event-by-event using incremental zlib deflate so the full
  # uncompressed body is never materialised in memory. Returns a single
  # gzip binary suitable for put_object (requires Content-Length upfront).
  @spec compress_to_binary([Message.t()]) :: binary()
  defp compress_to_binary(messages) do
    z = :zlib.open()
    :ok = :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)

    chunks =
      Enum.flat_map(messages, fn %{data: {id, tid}} ->
        case :ets.lookup(tid, id) do
          [{^id, _status, log_event}] ->
            :zlib.deflate(z, [Jason.encode!(log_event.body), "\n"], :none)

          [] ->
            []
        end
      end)

    final = :zlib.deflate(z, [], :finish)
    :zlib.deflateEnd(z)
    :zlib.close(z)

    IO.iodata_to_binary([chunks, final])
  end

  # Returns a Stream where each element is a binary >= @multipart_chunk_size
  # (except the last element, which may be smaller). Each element becomes one
  # S3 multipart part. Peak memory per batcher worker ≈ @multipart_chunk_size.
  @spec multipart_stream([Message.t()]) :: Enumerable.t()
  defp multipart_stream(messages) do
    Stream.resource(
      fn ->
        z = :zlib.open()
        :ok = :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)
        {:running, z, messages, [], 0}
      end,
      fn
        {:done, _z, _, _, _} = state ->
          {:halt, state}

        {:running, z, [], buffer, _size} ->
          final = :zlib.deflate(z, [], :finish)
          body = IO.iodata_to_binary([Enum.reverse(buffer), final])

          if byte_size(body) > 0 do
            {[body], {:done, z, [], [], 0}}
          else
            {:halt, {:done, z, [], [], 0}}
          end

        {:running, z, [%{data: {id, tid}} | rest], buffer, buf_size} ->
          compressed =
            case :ets.lookup(tid, id) do
              [{^id, _status, log_event}] ->
                IO.iodata_to_binary(
                  :zlib.deflate(z, [Jason.encode!(log_event.body), "\n"], :none)
                )

              [] ->
                <<>>
            end

          new_size = buf_size + byte_size(compressed)
          new_buffer = if compressed == <<>>, do: buffer, else: [compressed | buffer]

          if new_size >= @multipart_chunk_size do
            part = IO.iodata_to_binary(Enum.reverse(new_buffer))
            {[part], {:running, z, rest, [], 0}}
          else
            {[], {:running, z, rest, new_buffer, new_size}}
          end
      end,
      fn {_, z, _, _, _} ->
        :zlib.deflateEnd(z)
        :zlib.close(z)
      end
    )
  end

  # Generates a UUIDv7 string using the current millisecond timestamp plus
  # random bits. Files keyed by UUIDv7 sort chronologically within a partition,
  # enabling the future consumer to resume from a given offset.
  @spec generate_uuidv7() :: String.t()
  defp generate_uuidv7 do
    ms = System.system_time(:millisecond)

    # 12 random bits for rand_a (after the 4-bit version field)
    <<rand_a::12, _::4>> = :crypto.strong_rand_bytes(2)

    # 62 random bits for rand_b (after the 2-bit variant field)
    <<_::2, rand_b::62>> = :crypto.strong_rand_bytes(8)

    # Split 48-bit timestamp into two halves for UUID field layout
    <<time_high::32, time_mid::16>> = <<ms::48>>

    # Embed version (7) and variant (0b10) per RFC 9562
    ver_rand_a = 0x7000 ||| rand_a
    var_rand_b = 0x8000_0000_0000_0000 ||| rand_b

    hex = fn n, len ->
      n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(len, "0")
    end

    node = var_rand_b |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(16, "0")
    {clock_seq, node_str} = String.split_at(node, 4)

    "#{hex.(time_high, 8)}-#{hex.(time_mid, 4)}-#{hex.(ver_rand_a, 4)}-#{clock_seq}-#{node_str}"
  end
end
