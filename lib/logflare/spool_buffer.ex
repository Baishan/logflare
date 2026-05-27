defmodule Logflare.SpoolBuffer do
  @moduledoc """
  GenStage producer with a `:disk_log` overflow tier. Reads directly from
  IngestEventQueue (ETS) and can be used as a Broadway producer module.

  States:
    :passthrough - events pulled from ETS, buffered in memory, dispatched
    :spilling    - Broadway can't accept events fast enough; new events read
                   from ETS are written directly to disk_log
    :draining    - Broadway recovered; disk_log is read back into memory
                   until empty, then transitions back to :passthrough
  """

  use GenStage

  require Logger

  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent

  @log_name :logflare_spool
  @high_watermark 200
  @low_watermark  50
  @stall_ms       2_000
  @drain_chunk    1_000

  defstruct [
    :log,
    :source_id,
    :backend_id,
    mode: :passthrough,
    mem: :queue.new(),
    mem_size: 0,
    pending_demand: 0,
    last_dispatch_at: nil
  ]

  def start_link(opts) do
    gen_opts = if name = Keyword.get(opts, :name), do: [name: name], else: []
    GenStage.start_link(__MODULE__, opts, gen_opts)
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    source_id = Keyword.fetch!(opts, :source_id)
    backend_id = Keyword.get(opts, :backend_id)

    path = spool_path(source_id)
    File.mkdir_p!(Path.dirname(path))

    log =
      case :disk_log.open(
             name: {@log_name, source_id},
             file: String.to_charlist(path),
             type: :halt,
             format: :internal,
             repair: true
           ) do
        {:ok, log} ->
          log

        {:repaired, log, {:recovered, recovered}, {:badbytes, bad}} ->
          if bad > 0 do
            Logger.warning("spool: disk log repaired",
              source_id: source_id,
              recovered: recovered,
              badbytes: bad
            )
          end

          log
      end

    # Register with IngestEventQueue so the router delivers events here.
    table_key = {source_id, backend_id, self()}
    startup_table_key = {source_id, backend_id, nil}
    IngestEventQueue.upsert_tid(table_key)
    IngestEventQueue.move(startup_table_key, table_key)

    # Periodic tick to detect downstream stalls and to drive draining.
    :timer.send_interval(250, :tick)

    state = %__MODULE__{
      log: log,
      source_id: source_id,
      backend_id: backend_id,
      last_dispatch_at: now_ms()
    }

    {:producer, state}
  end

  # ── Downstream demand ──────────────────────────────────────────────────────

  # Fast path: Broadway is asking for events. Read from ETS immediately to
  # fulfil demand rather than waiting for the next tick.
  @impl true
  def handle_demand(demand, s) do
    s = %{s | pending_demand: s.pending_demand + demand}
    s = read_ets(s)
    dispatch(s)
  end

  # ── Periodic tick: pre-fill buffer, detect stalls, drain disk ─────────────

  @impl true
  def handle_info(:tick, s) do
    s = maybe_detect_stall(s)
    s = maybe_drain_from_disk(s)
    s = read_ets(s)
    dispatch(s)
  end

  @impl true
  def handle_info({:EXIT, _pid, _reason}, s) do
    key = {s.source_id, s.backend_id, self()}
    startup_key = {s.source_id, s.backend_id, nil}
    IngestEventQueue.move(key, startup_key)
    {:noreply, [], s}
  end

  # ── Internals ──────────────────────────────────────────────────────────────

  # Passthrough + demand outstanding: read from ETS up to pending_demand to
  # satisfy Broadway directly. Keeps latency low when Broadway is keeping up.
  defp read_ets(%{mode: :passthrough, pending_demand: demand} = s) when demand > 0 do
    key = {s.source_id, s.backend_id, self()}

    case IngestEventQueue.pop_pending(key, min(demand, @drain_chunk)) do
      {:ok, []} -> s
      {:ok, events} ->
        events = Enum.map(events, fn %LogEvent{} = e -> %{e | is_popped: true} end)
        enqueue(s, events)
      _ -> s
    end
  end

  # Passthrough + no demand: pre-fill memory buffer up to the watermark so
  # that events are available the moment Broadway asks. If memory fills while
  # Broadway still has no demand, dispatch/1 will spill it to disk.
  defp read_ets(%{mode: :passthrough} = s) do
    headroom = max(0, @high_watermark - s.mem_size)
    if headroom == 0, do: s, else: do_fill(s, headroom)
  end

  # Spilling: Broadway can't keep up; pull from ETS and write straight to disk.
  defp read_ets(%{mode: :spilling} = s) do
    key = {s.source_id, s.backend_id, self()}

    case IngestEventQueue.pop_pending(key, @drain_chunk) do
      {:ok, []} ->
        s

      {:ok, events} ->
        events = Enum.map(events, fn %LogEvent{} = e -> %{e | is_popped: true} end)
        :ok = :disk_log.log_terms(s.log, events)
        Logger.debug("spool: wrote #{length(events)} events to disk", source_id: s.source_id)
        s

      _ ->
        s
    end
  end

  # Draining: disk is being read back into memory; don't read more from ETS.
  defp read_ets(s), do: s

  defp do_fill(s, count) do
    key = {s.source_id, s.backend_id, self()}

    case IngestEventQueue.pop_pending(key, count) do
      {:ok, []} -> s
      {:ok, events} ->
        events = Enum.map(events, fn %LogEvent{} = e -> %{e | is_popped: true} end)
        enqueue(s, events)
      _ -> s
    end
  end

  defp enqueue(s, events) do
    q = Enum.reduce(events, s.mem, &:queue.in/2)
    %{s | mem: q, mem_size: s.mem_size + length(events)}
  end

  # Broadway is stalled (no demand) and memory is full — flush to disk.
  # This is the real spill trigger: it fires only when Broadway genuinely
  # can't accept events, not transiently during a demand/dispatch cycle.
  defp dispatch(%{pending_demand: 0, mode: :passthrough, mem_size: size} = s)
       when size >= @high_watermark do
    Logger.warning("spool: Broadway stalled, spilling #{size} events to disk",
      source_id: s.source_id
    )

    events = :queue.to_list(s.mem)
    :ok = :disk_log.log_terms(s.log, events)
    {:noreply, [], %{s | mode: :spilling, mem: :queue.new(), mem_size: 0}}
  end

  defp dispatch(%{pending_demand: 0} = s), do: {:noreply, [], s}

  defp dispatch(s) do
    {to_send, q_rest, taken} = take(s.mem, s.pending_demand, [], 0)

    s = %{
      s
      | mem: q_rest,
        mem_size: s.mem_size - taken,
        pending_demand: s.pending_demand - taken,
        last_dispatch_at: now_ms()
    }

    s = maybe_exit_spilling(s)
    {:noreply, to_send, s}
  end

  defp take(q, 0, acc, n), do: {Enum.reverse(acc), q, n}
  defp take(q, demand, acc, n) do
    case :queue.out(q) do
      {{:value, ev}, q2} -> take(q2, demand - 1, [ev | acc], n + 1)
      {:empty, q2}       -> {Enum.reverse(acc), q2, n}
    end
  end

  # Stall detected: flush in-memory events to disk and enter :spilling.
  defp maybe_detect_stall(%{mode: :passthrough, mem_size: size} = s) when size > 0 do
    if now_ms() - s.last_dispatch_at > @stall_ms do
      Logger.warning("spool: downstream stalled, entering :spilling",
        source_id: s.source_id
      )

      events = :queue.to_list(s.mem)
      :ok = :disk_log.log_terms(s.log, events)
      %{s | mode: :spilling, mem: :queue.new(), mem_size: 0}
    else
      s
    end
  end

  defp maybe_detect_stall(s), do: s

  defp maybe_exit_spilling(%{mode: :spilling, mem_size: size} = s)
       when size <= @low_watermark do
    Logger.info("spool: mem drained, entering :draining", source_id: s.source_id)
    %{s | mode: :draining}
  end

  defp maybe_exit_spilling(s), do: s

  defp maybe_drain_from_disk(%{mode: :draining, mem_size: size} = s)
       when size <= @low_watermark do
    case read_chunk(s.log, @drain_chunk) do
      {:ok, []} ->
        Logger.info("spool: disk drained, back to :passthrough",
          source_id: s.source_id
        )

        %{s | mode: :passthrough}

      {:ok, events} ->
        :ok = truncate_read(s.log, length(events))
        enqueue(s, events)
    end
  end

  defp maybe_drain_from_disk(s), do: s

  defp read_chunk(log, n) do
    case :disk_log.chunk(log, :start, n) do
      :eof              -> {:ok, []}
      {_cont, terms}    -> {:ok, terms}
      {_cont, terms, _} -> {:ok, terms}
      {:error, reason}  -> {:error, reason}
    end
  end

  defp truncate_read(_log, _n) do
    # Placeholder — see "what's hand-wavy" below.
    :ok
  end

  defp spool_path(source_id),
    do: Path.join(["/Users/brian/supabase/logflare/spool/spool", "#{source_id}.log"])

  defp now_ms, do: System.monotonic_time(:millisecond)
end
