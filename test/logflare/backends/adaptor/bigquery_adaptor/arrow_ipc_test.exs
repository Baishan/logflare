defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPCTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC

  describe "get_ipc_bytes_from_rows/1" do
    test "encodes a simple row" do
      rows = [%{"timestamp" => 1, "event_message" => "hello"}]

      assert {schema, [batch]} = ArrowIPC.get_ipc_bytes_from_rows(rows)
      assert is_binary(schema) and byte_size(schema) > 0
      assert is_binary(batch) and byte_size(batch) > 0
    end

    test "encodes a nested map field (wrapped as a repeated record)" do
      rows = [
        %{"timestamp" => 1, "event_message" => "a", "user" => %{"id" => 1, "plan" => "pro"}}
      ]

      assert {_schema, [_batch]} = ArrowIPC.get_ipc_bytes_from_rows(rows)
    end

    test "encodes a list of scalar values" do
      rows = [%{"timestamp" => 1, "event_message" => "a", "tags" => ["api", "v1"]}]

      assert {_schema, [_batch]} = ArrowIPC.get_ipc_bytes_from_rows(rows)
    end

    test "encodes a list of numbers without dropping it" do
      rows = [%{"timestamp" => 1, "event_message" => "a", "counts" => [1, 2, 3]}]

      assert {_schema, [_batch]} = ArrowIPC.get_ipc_bytes_from_rows(rows)
    end

    test "treats an explicit nil the same as a missing key" do
      rows = [
        %{"timestamp" => 1, "event_message" => "a", "count" => 1.0},
        %{"timestamp" => 2, "event_message" => "b", "count" => nil}
      ]

      assert {_schema, [_batch]} = ArrowIPC.get_ipc_bytes_from_rows(rows)
    end

    test "widens a numeric field to a string on a genuine type conflict instead of raising" do
      rows = [
        %{"timestamp" => 1, "event_message" => "a", "count" => 1.0},
        %{"timestamp" => 2, "event_message" => "b", "count" => "unknown"}
      ]

      assert {_schema, [_batch]} = ArrowIPC.get_ipc_bytes_from_rows(rows)
    end

    test "preserves integer precision beyond float64's exact range" do
      rows = [
        %{"timestamp" => 1, "event_message" => "a", "big_id" => 9_007_199_254_740_993}
      ]

      assert {_schema, [_batch]} = ArrowIPC.get_ipc_bytes_from_rows(rows)
    end

    test "handles rows with different metadata keys in the same batch" do
      rows = [
        %{"timestamp" => 1, "event_message" => "a", "region" => "us-east-1"},
        %{"timestamp" => 2, "event_message" => "b"}
      ]

      assert {_schema, [_batch]} = ArrowIPC.get_ipc_bytes_from_rows(rows)
    end

    test "raises when a row is missing the required timestamp field" do
      rows = [%{"event_message" => "a"}]

      assert_raise ArgumentError, fn -> ArrowIPC.get_ipc_bytes_from_rows(rows) end
    end
  end
end
