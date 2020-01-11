defmodule Broadway.Processor do
  @moduledoc false
  use GenStage

  require Logger
  alias Broadway.{Message, Acknowledger}

  @spec start_link(term, GenServer.options()) :: GenServer.on_start()
  def start_link(args, stage_options) do
    Broadway.Subscriber.start_link(
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
      name: args[:name],
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
    {successful_messages, failed_messages} = handle_messages(messages, [], [], state)

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

    {:noreply, successful_messages_to_forward, state}
  end

  defp handle_messages([message | messages], successful, failed, state) do
    %{
      module: module,
      context: context,
      processor_key: processor_key,
      batchers: batchers,
      name: name
    } = state

    start_time = System.monotonic_time()

    telemetry_metadata = %{
      module: module,
      processor_key: processor_key,
      name: name,
      message: message
    }

    try do
      emit_start_event(start_time, telemetry_metadata)

      handle_result =
        module.handle_message(processor_key, message, context)
        |> validate_message(batchers)

      emit_stop_event(start_time, telemetry_metadata)

      handle_result
    catch
      kind, reason ->
        reason = Exception.normalize(kind, reason, __STACKTRACE__)

        emit_error_event(start_time, telemetry_metadata, reason)

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

  defp emit_start_event(start_time, metadata) do
    :telemetry.execute([:broadway, :processor, :start], %{time: start_time}, metadata)
  end

  defp emit_stop_event(start_time, metadata) do
    stop_time = System.monotonic_time()
    measurements = %{time: stop_time, duration: stop_time - start_time}

    :telemetry.execute([:broadway, :processor, :stop], measurements, metadata)
  end

  def emit_error_event(start_time, metadata, error_reason) do
    stop_time = System.monotonic_time()
    measurements = %{time: stop_time, duration: stop_time - start_time}
    metadata_with_error = Map.put(metadata, :error, error_reason)

    :telemetry.execute([:broadway, :processor, :error], measurements, metadata_with_error)
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
end
