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

    Broadway.Subscriber.init(
      [args[:batcher]],
      @subscription_options,
      state,
      args
    )
  end

  @impl true
  def handle_events(events, _from, state) do
    [{messages, batch_info}] = events
    %Broadway.BatchInfo{batcher: batcher, size: size} = batch_info

    {successful_messages, failed_messages, returned} =
      handle_batch(batcher, messages, batch_info, state)

    failed_messages = maybe_handle_failed_messages(failed_messages, state, size)

    if returned != size do
      Logger.error "#{inspect state.module}.handle_batch/4 received #{size} messages and " <>
                     "returned only #{returned}. All messages given to handle_batch/4 " <>
                     "must be returned"
    end

    try do
      Acknowledger.ack_messages(successful_messages, failed_messages)
    catch
      kind, reason ->
        Logger.error(Exception.format(kind, reason, System.stacktrace()))
    end

    {:noreply, [], state}
  end

  defp maybe_handle_failed_messages(messages, %{module: module, context: context}, size) do
    if function_exported?(module, :handle_failed, 2) do
      handle_failed_messages(messages, module, context, size)
    else
      messages
    end
  end

  defp handle_failed_messages(messages, module, context, size) do
    module.handle_failed(messages, context)
  catch
    kind, reason ->
      Logger.error(Exception.format(kind, reason, System.stacktrace()))
      failed = "due to an unhandled #{kind} in handle_failed/2"
      Enum.map(messages, &Message.failed(&1, failed))
  else
    messages ->
      returned_size = length(messages)

      if returned_size != size do
        Logger.error "#{inspect module}.handle_failed/2 received #{size} messages and " <>
                        "returned only #{returned_size}. All messages given to handle_failed/2 " <>
                        "must be returned"
      end

      messages
  end

  defp handle_batch(batcher, messages, batch_info, state) do
    %{module: module, context: context} = state

    try do
      module.handle_batch(batcher, messages, batch_info, context)
      |> split_by_status([], [], 0)
    catch
      kind, reason ->
        Logger.error(Exception.format(kind, reason, System.stacktrace()))
        failed = "due to an unhandled #{kind}"
        {[], Enum.map(messages, &Message.failed(&1, failed)), batch_info.size}
    end
  end

  defp split_by_status([], successful, failed, count) do
    {Enum.reverse(successful), Enum.reverse(failed), count}
  end

  defp split_by_status([%Message{status: :ok} = message | rest], successful, failed, count) do
    split_by_status(rest, [message | successful], failed, count + 1)
  end

  defp split_by_status([%Message{} = message | rest], successful, failed, count) do
    split_by_status(rest, successful, [message | failed], count + 1)
  end
end
