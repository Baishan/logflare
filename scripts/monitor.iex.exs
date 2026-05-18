import_if_available(Logflare.Utils.Debugging)

source_name = System.get_env("MONITOR_SOURCE", "loadfest.test.0")
baseline_proc = :erlang.memory(:processes)

IO.puts("Starting memory monitor for source: #{source_name}")
IO.puts("Baseline proc=#{Float.round(baseline_proc / 1_048_576, 1)}MB\n")

spawn(fn ->
  Stream.repeatedly(fn ->
    ets = :erlang.memory(:ets)
    proc = :erlang.memory(:processes)
    total = :erlang.memory(:total)
    proc_delta = proc - baseline_proc

    source = Logflare.Sources.get_by(name: source_name)

    {pending, ingested_size} =
      if source do
        pending = Logflare.Backends.IngestEventQueue.total_pending({source.id, nil})
        ingested_size = Logflare.Backends.IngestEventQueue.get_table_size({source.id, nil, nil})
        {pending, ingested_size}
      else
        {:"source_not_found", :"source_not_found"}
      end

    IO.puts(
      "ets=#{Float.round(ets / 1_048_576, 1)}MB " <>
        "proc=#{Float.round(proc / 1_048_576, 1)}MB " <>
        "total=#{Float.round(total / 1_048_576, 1)}MB " <>
        "proc_delta=#{Float.round(proc_delta / 1_048_576, 1)}MB | " <>
        "queue_size=#{ingested_size} pending=#{pending}"
    )

    Process.sleep(1_000)
  end)
  |> Stream.run()
end)
