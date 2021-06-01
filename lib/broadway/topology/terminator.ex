defmodule Broadway.Topology.Terminator do
  @moduledoc false
  use GenServer

  @spec start_link(term, GenServer.options()) :: GenServer.on_start()
  def start_link(args, opts) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  @spec trap_exit(GenServer.server()) :: :ok
  def trap_exit(terminator) do
    GenServer.cast(terminator, :trap_exit)
  end

  @impl true
  def init(args) do
    state = %{
      producers: args[:producers],
      first: args[:first],
      last: args[:last]
    }

    {:ok, state}
  end

  @impl true
  def handle_cast(:trap_exit, state) do
    Process.flag(:trap_exit, true)
    {:noreply, state}
  end

  @impl true
  def handle_info(_, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_, state) do
    for name <- state.first, pid = GenServer.whereis(name) do
      send(pid, :will_terminate)
    end

    for name <- state.producers, pid = GenServer.whereis(name) do
      Broadway.Topology.ProducerStage.drain(pid)
    end

    for name <- state.last, pid = GenServer.whereis(name) do
      ref = Process.monitor(pid)

      receive do
        {:done, ^pid} -> :ok
        {:DOWN, ^ref, _, _, _} -> :ok
      end
    end

    :ok
  end
end
