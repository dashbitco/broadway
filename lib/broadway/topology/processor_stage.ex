defmodule Broadway.Topology.ProcessorStage do
  @moduledoc false
  use GenStage

  require Logger
  alias Broadway.{Message, Acknowledger}

  @spec start_link(term, GenServer.options()) :: GenServer.on_start()
  def start_link(args, stage_options) do
    Broadway.Topology.Subscriber.start_link(
      __MODULE__,
      args[:producers],
      args,
      Keyword.take(args[:processor_config], [:min_demand, :max_demand]),
      stage_options
    )
  end

  @impl true
  def init(args) do
    Process.flag(:trap_exit, true)
    type = args[:type]

    state = %{
      topology_name: args[:topology_name],
      name: args[:name],
      partition: args[:partition],
      type: type,
      module: args[:module],
      context: args[:context],
      processor_key: args[:processor_key],
      batchers: args[:batchers]
    }

    case type do
      :consumer ->
        {:consumer, state, []}

      :producer_consumer ->
        {:producer_consumer, state, dispatcher: args[:dispatcher]}
    end
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  @impl true
  def handle_events(messages, _from, state) do
    start_time = System.monotonic_time()
    emit_start_event(state, start_time, messages)

    {prepared_messages, prepared_failed_messages} = maybe_prepare_messages(messages, state)
    {successful_messages, failed_messages} = handle_messages(prepared_messages, [], [], state)
    failed_messages = prepared_failed_messages ++ failed_messages

    {successful_messages_to_forward, successful_messages_to_ack} =
      case state do
        %{type: :consumer} ->
          {[], successful_messages}

        %{} ->
          {successful_messages, []}
      end

    failed_messages =
      Acknowledger.maybe_handle_failed_messages(
        failed_messages,
        state.module,
        state.context
      )

    try do
      Acknowledger.ack_messages(successful_messages_to_ack, failed_messages)
    catch
      kind, reason ->
        Logger.error(Exception.format(kind, reason, __STACKTRACE__),
          crash_reason: Acknowledger.crash_reason(kind, reason, __STACKTRACE__)
        )
    end

    emit_stop_event(
      state,
      start_time,
      successful_messages_to_ack,
      successful_messages_to_forward,
      failed_messages
    )

    {:noreply, successful_messages_to_forward, state}
  end

  defp emit_start_event(state, start_time, messages) do
    metadata = %{
      topology_name: state.topology_name,
      name: state.name,
      index: state.partition,
      processor_key: state.processor_key,
      messages: messages
    }

    measurements = %{time: start_time}

    :telemetry.execute([:broadway, :processor, :start], measurements, metadata)
  end

  defp emit_stop_event(
         state,
         start_time,
         successful_messages_to_ack,
         successful_messages_to_forward,
         failed_messages
       ) do
    metadata = %{
      topology_name: state.topology_name,
      name: state.name,
      index: state.partition,
      successful_messages_to_ack: successful_messages_to_ack,
      successful_messages_to_forward: successful_messages_to_forward,
      processor_key: state.processor_key,
      failed_messages: failed_messages
    }

    stop_time = System.monotonic_time()
    measurements = %{time: stop_time, duration: stop_time - start_time}
    :telemetry.execute([:broadway, :processor, :stop], measurements, metadata)
  end

  defp maybe_prepare_messages(messages, state) do
    %{module: module, context: context} = state

    if function_exported?(module, :prepare_messages, 2) do
      try do
        prepared_messages =
          messages
          |> module.prepare_messages(context)
          |> validate_prepared_messages(messages)

        {prepared_messages, []}
      catch
        kind, reason ->
          reason = Exception.normalize(kind, reason, __STACKTRACE__)

          Logger.error(Exception.format(kind, reason, __STACKTRACE__),
            crash_reason: Acknowledger.crash_reason(kind, reason, __STACKTRACE__)
          )

          messages = Enum.map(messages, &%{&1 | status: {kind, reason, __STACKTRACE__}})
          {[], messages}
      end
    else
      {messages, []}
    end
  end

  defp handle_messages([message | messages], successful, failed, state) do
    %{
      module: module,
      context: context,
      processor_key: processor_key,
      batchers: batchers
    } = state

    start_time = System.monotonic_time()
    emit_message_start_event(start_time, state, message)

    try do
      message =
        module.handle_message(processor_key, message, context)
        |> validate_message(batchers)

      emit_message_stop_event(start_time, state, message)
      message
    catch
      kind, reason ->
        reason = Exception.normalize(kind, reason, __STACKTRACE__)

        emit_message_failure_event(
          start_time,
          state,
          message,
          kind,
          reason,
          __STACKTRACE__
        )

        Logger.error(Exception.format(kind, reason, __STACKTRACE__),
          crash_reason: Acknowledger.crash_reason(kind, reason, __STACKTRACE__)
        )

        message = %{message | status: {kind, reason, __STACKTRACE__}}
        handle_messages(messages, successful, [message | failed], state)
    else
      %{status: :ok} = message ->
        handle_messages(messages, [message | successful], failed, state)

      %{status: {:failed, _}} = message ->
        handle_messages(messages, successful, [message | failed], state)
    end
  end

  defp handle_messages([], successful, failed, _state) do
    {Enum.reverse(successful), Enum.reverse(failed)}
  end

  defp emit_message_start_event(start_time, state, message) do
    metadata = %{
      processor_key: state.processor_key,
      topology_name: state.topology_name,
      index: state.partition,
      name: state.name,
      message: message
    }

    :telemetry.execute([:broadway, :processor, :message, :start], %{time: start_time}, metadata)
  end

  defp emit_message_stop_event(start_time, state, message) do
    stop_time = System.monotonic_time()
    measurements = %{time: stop_time, duration: stop_time - start_time}

    metadata = %{
      processor_key: state.processor_key,
      topology_name: state.topology_name,
      index: state.partition,
      name: state.name,
      message: message
    }

    :telemetry.execute([:broadway, :processor, :message, :stop], measurements, metadata)
  end

  defp emit_message_failure_event(
         start_time,
         state,
         message,
         kind,
         reason,
         stacktrace
       ) do
    stop_time = System.monotonic_time()
    measurements = %{time: stop_time, duration: stop_time - start_time}

    metadata = %{
      processor_key: state.processor_key,
      topology_name: state.topology_name,
      name: state.name,
      index: state.partition,
      message: message,
      kind: kind,
      reason: reason,
      stacktrace: stacktrace
    }

    :telemetry.execute([:broadway, :processor, :message, :exception], measurements, metadata)
  end

  defp validate_message(%Message{batcher: batcher, status: status} = message, batchers) do
    if status == :ok and batchers != :none and batcher not in batchers do
      raise "message was set to unknown batcher #{inspect(batcher)}. " <>
              "The known batchers are #{inspect(batchers)}"
    end

    message
  end

  defp validate_message(message, _batchers) do
    raise "expected a Broadway.Message from handle_message/3, got #{inspect(message)}"
  end

  defp validate_prepared_messages(prepared_messages, messages) do
    if length(prepared_messages) != length(messages) do
      raise "expected all messages to be returned from prepared_messages/2"
    end

    prepared_messages
  end
end
