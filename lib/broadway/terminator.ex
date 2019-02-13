defmodule Broadway.Terminator do
  @moduledoc false
  use GenServer

  def start_link(args, opts) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def trap_exit(terminator) do
    GenServer.cast(terminator, :trap_exit)
  end

  def init(args) do
    state = %{
      producers: args[:producers],
      first: args[:first],
      last: args[:last]
    }

    {:ok, state}
  end

  def handle_cast(:trap_exit, state) do
    Process.flag(:trap_exit, true)
    {:noreply, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_, state) do
    for name <- state.first, pid = Process.whereis(name) do
      send(pid, :never_resubscribe)
    end

    for name <- state.producers, pid = Process.whereis(name) do
      GenStage.demand(pid, :accumulate)
      GenStage.async_info(pid, :cancel_consumers)
    end

    for name <- state.last, pid = Process.whereis(name) do
      ref = Process.monitor(pid)

      receive do
        {:done, ^pid} -> :ok
        {:DOWN, ^ref, _, _, _} -> :ok
      end
    end

    :ok
  end
end
