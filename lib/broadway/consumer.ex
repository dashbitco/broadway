defmodule Broadway.Consumer do
  @moduledoc false
  use GenStage
  use Broadway.Subscriber

  require Logger

  alias Broadway.{Acknowledger, Message}
  @subscription_options [max_demand: 1, min_demand: 0]

  @spec start_link(term, GenServer.options()) :: GenServer.on_start()
  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(args) do
    Process.flag(:trap_exit, true)

    state = %{
      module: args[:module],
      context: args[:context]
    }

    broadway_index = args[:broadway_index]
    partition_options = [partition: broadway_index]

    Broadway.Subscriber.init(
      [args[:batcher]],
      @subscription_options ++ partition_options,
      state,
      args
    )
  end

  @impl true
  def handle_events(events, _from, state) do
    [{messages, batch_info}] = events
    %Broadway.BatchInfo{batcher: batcher} = batch_info

    {successful_messages, failed_messages} = handle_batch(batcher, messages, batch_info, state)

    try do
      Acknowledger.ack_messages(successful_messages, failed_messages)
    catch
      kind, reason ->
        Logger.error(Exception.format(kind, reason, System.stacktrace()))
    end

    {:noreply, [], state}
  end

  defp handle_batch(batcher, messages, batch_info, state) do
    %{module: module, context: context} = state

    try do
      module.handle_batch(batcher, messages, batch_info, context)
      |> Enum.split_with(fn %Message{status: status} -> status == :ok end)
    catch
      kind, reason ->
        Logger.error(Exception.format(kind, reason, System.stacktrace()))
        failed = "due to an unhandled #{kind}"
        {[], Enum.map(messages, &Message.failed(&1, failed))}
    end
  end
end
