defmodule Broadway.Consumer do
  @moduledoc false
  use GenStage
  require Logger
  alias Broadway.{Acknowledger, Message}
  @subscription_options [max_demand: 1, min_demand: 0]

  @spec start_link(term, GenServer.options()) :: GenServer.on_start()
  def start_link(args, stage_options) do
    Broadway.Subscriber.start_link(
      __MODULE__,
      [args[:batcher]],
      args,
      @subscription_options,
      stage_options
    )
  end

  @impl true
  def init(args) do
    Process.flag(:trap_exit, true)

    state = %{
      name: args[:name],
      module: args[:module],
      context: args[:context]
    }

    {:consumer, state, []}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  @impl true
  def handle_events(events, _from, state) do
    [{messages, batch_info}] = events
    %Broadway.BatchInfo{batcher: batcher, size: size} = batch_info

    start_time = System.monotonic_time()
    emit_start_event(state.name, start_time, messages, batch_info)

    {successful_messages, failed_messages, returned} =
      handle_batch(batcher, messages, batch_info, state)

    failed_messages =
      Acknowledger.maybe_handle_failed_messages(
        failed_messages,
        state.module,
        state.context
      )

    if returned != size do
      Logger.error(
        "#{inspect(state.module)}.handle_batch/4 received #{size} messages and " <>
          "returned only #{returned}. All messages given to handle_batch/4 " <>
          "must be returned"
      )
    end

    try do
      Acknowledger.ack_messages(successful_messages, failed_messages)
    catch
      kind, reason ->
        Logger.error(Exception.format(kind, reason, __STACKTRACE__),
          crash_reason: Acknowledger.crash_reason(kind, reason, __STACKTRACE__)
        )
    end

    emit_stop_event(state.name, start_time, successful_messages, failed_messages, batch_info)
    {:noreply, [], state}
  end

  defp emit_start_event(name, start_time, messages, batch_info) do
    metadata = %{name: name, messages: messages, batch_info: batch_info}
    measurements = %{time: start_time}
    :telemetry.execute([:broadway, :consumer, :start], measurements, metadata)
  end

  defp emit_stop_event(name, start_time, successful_messages, failed_messages, batch_info) do
    metadata = %{
      name: name,
      successful_messages: successful_messages,
      failed_messages: failed_messages,
      batch_info: batch_info
    }

    stop_time = System.monotonic_time()
    measurements = %{time: stop_time, duration: stop_time - start_time}
    :telemetry.execute([:broadway, :consumer, :stop], measurements, metadata)
  end

  defp handle_batch(batcher, messages, batch_info, state) do
    %{module: module, context: context} = state

    try do
      module.handle_batch(batcher, messages, batch_info, context)
      |> split_by_status([], [], 0)
    catch
      kind, reason ->
        reason = Exception.normalize(kind, reason, __STACKTRACE__)

        Logger.error(Exception.format(kind, reason, __STACKTRACE__),
          crash_reason: Acknowledger.crash_reason(kind, reason, __STACKTRACE__)
        )

        messages = Enum.map(messages, &%{&1 | status: {kind, reason, __STACKTRACE__}})
        {[], messages, batch_info.size}
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
