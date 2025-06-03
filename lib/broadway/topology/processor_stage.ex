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
      batchers: args[:batchers],
      producer: args[:producer]
    }

    case type do
      :consumer ->
        {:consumer, state, []}

      :producer_consumer ->
        {:producer_consumer, state, dispatcher: args[:dispatcher]}
    end
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) when reason not in [:normal, :shutdown] do
    Logger.error(
      "Processor received a trapped exit from #{inspect(pid)} with reason: " <>
        Exception.format_exit(reason)
    )

    {:noreply, [], state}
  end

  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  @impl true
  def handle_events(messages, _from, state) do
    :telemetry.span(
      [:broadway, :processor],
      %{
        topology_name: state.topology_name,
        name: state.name,
        index: state.partition,
        processor_key: state.processor_key,
        messages: messages,
        context: state.context,
        producer: state.producer
      },
      fn ->
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

        {{:noreply, successful_messages_to_forward, state},
         %{
           topology_name: state.topology_name,
           name: state.name,
           index: state.partition,
           successful_messages_to_ack: successful_messages_to_ack,
           successful_messages_to_forward: successful_messages_to_forward,
           processor_key: state.processor_key,
           failed_messages: failed_messages,
           context: state.context,
           producer: state.producer
         }}
      end
    )
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

    {successful, failed} =
      try do
        :telemetry.span(
          [:broadway, :processor, :message],
          %{
            processor_key: state.processor_key,
            topology_name: state.topology_name,
            index: state.partition,
            name: state.name,
            message: message,
            context: state.context
          },
          fn ->
            updated_message =
              processor_key
              |> module.handle_message(message, context)
              |> validate_message(batchers)

            {updated_message,
             %{
               processor_key: state.processor_key,
               topology_name: state.topology_name,
               index: state.partition,
               name: state.name,
               message: updated_message,
               context: state.context
             }}
          end
        )
      catch
        kind, reason ->
          reason = Exception.normalize(kind, reason, __STACKTRACE__)

          Logger.error(Exception.format(kind, reason, __STACKTRACE__),
            crash_reason: Acknowledger.crash_reason(kind, reason, __STACKTRACE__)
          )

          message = %{message | status: {kind, reason, __STACKTRACE__}}
          {successful, [message | failed]}
      else
        %{status: :ok} = message ->
          {[message | successful], failed}

        %{status: {:failed, _}} = message ->
          {successful, [message | failed]}
      end

    handle_messages(messages, successful, failed, state)
  end

  defp handle_messages([], successful, failed, _state) do
    {Enum.reverse(successful), Enum.reverse(failed)}
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
