defmodule Logflare.Poc.UserInfo do
  @moduledoc false

  use Protobuf,
    full_name: "logflare.poc.UserInfo",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field :id, 1, type: :int64
  field :plan, 2, type: :string
end

defmodule Logflare.Poc.LogRow do
  @moduledoc false

  use Protobuf,
    full_name: "logflare.poc.LogRow",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field :timestamp_us, 1, type: :int64, json_name: "timestampUs"
  field :event_message, 2, type: :string, json_name: "eventMessage"
  field :request_id, 3, type: :string, json_name: "requestId"
  field :user, 4, type: Logflare.Poc.UserInfo
  field :tags, 5, repeated: true, type: :string
end

defmodule Logflare.Poc.LogRows do
  @moduledoc false

  use Protobuf,
    full_name: "logflare.poc.LogRows",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field :rows, 1, repeated: true, type: Logflare.Poc.LogRow
end

defmodule Logflare.Poc.Value do
  @moduledoc false

  use Protobuf,
    full_name: "logflare.poc.Value",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  oneof :kind, 0

  field :string_value, 1, type: :string, json_name: "stringValue", oneof: 0
  field :number_value, 2, type: :double, json_name: "numberValue", oneof: 0
  field :bool_value, 3, type: :bool, json_name: "boolValue", oneof: 0
  field :list_value, 4, type: Logflare.Poc.StringList, json_name: "listValue", oneof: 0
end

defmodule Logflare.Poc.StringList do
  @moduledoc false

  use Protobuf,
    full_name: "logflare.poc.StringList",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field :values, 1, repeated: true, type: :string
end

defmodule Logflare.Poc.LogRowDynamic.MetadataEntry do
  @moduledoc false

  use Protobuf,
    full_name: "logflare.poc.LogRowDynamic.MetadataEntry",
    map: true,
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field :key, 1, type: :string
  field :value, 2, type: Logflare.Poc.Value
end

defmodule Logflare.Poc.LogRowDynamic do
  @moduledoc false

  use Protobuf,
    full_name: "logflare.poc.LogRowDynamic",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field :timestamp_us, 1, type: :int64, json_name: "timestampUs"
  field :event_message, 2, type: :string, json_name: "eventMessage"
  field :metadata, 3, repeated: true, type: Logflare.Poc.LogRowDynamic.MetadataEntry, map: true
end

defmodule Logflare.Poc.LogRowsDynamic do
  @moduledoc false

  use Protobuf,
    full_name: "logflare.poc.LogRowsDynamic",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field :rows, 1, repeated: true, type: Logflare.Poc.LogRowDynamic
end
