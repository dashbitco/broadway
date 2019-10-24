defmodule Broadway.Processor do
  @moduledoc false
  use GenStage
  use Broadway.Subscriber

  require Logger
  alias Broadway.{Message, Acknowledger}

  @spec start_link(term, GenServer.options()) :: GenServer.on_start()
  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(args) do
    Process.flag(:trap_exit, true)
    processor_config = args[:processor_config]

    state = %{
      module: args[:module],
      context: args[:context],
      processor_key: args[:processor_key],
      batchers: args[:batchers]
    }

    Broadway.Subscriber.init(
      args[:producers],
      Keyword.take(processor_config, [:min_demand, :max_demand]),
      state,
      args
    )
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

    try do
      Acknowledger.ack_messages(successful_messages_to_ack, failed_messages)
    catch
      kind, error ->
        Logger.error(Exception.format(kind, error, System.stacktrace()))
    end

    {:noreply, successful_messages_to_forward, state}
  end

  defp handle_messages([message | messages], successful, failed, state) do
    %{
      module: module,
      context: context,
      processor_key: processor_key,
      batchers: batchers
    } = state

    try do
      module.handle_message(processor_key, message, context)
      |> validate_message(batchers)
    catch
      kind, error ->
        Logger.error(Exception.format(kind, error, System.stacktrace()))
        message = Message.failed(message, "due to an unhandled #{kind}")
        message = maybe_handle_failed_message(message, module, context)
        handle_messages(messages, successful, [message | failed], state)
    else
      %{status: {:failed, _}} = message ->
        message = maybe_handle_failed_message(message, module, context)
        handle_messages(messages, successful, [message | failed], state)

      message ->
        handle_messages(messages, [message | successful], failed, state)
    end
  end

  defp handle_messages([], successful, failed, _state) do
    {Enum.reverse(successful), Enum.reverse(failed)}
  end

  defp maybe_handle_failed_message(message, module, context) do
    if function_exported?(module, :handle_failed, 2) do
      handle_failed_message(message, module, context)
    else
      message
    end
  end

  defp handle_failed_message(message, module, context) do
    module.handle_failed([message], context)
  catch
    kind, error ->
      Logger.error(Exception.format(kind, error, System.stacktrace()))
      Message.failed(message, "due to unhandled #{kind} in handle_failed")
  else
    [message] ->
      message

    _other ->
      Logger.error(
        "#{inspect(module)}.handle_failed/2 got one message and should return one message, " <>
          "ignoring its return value"
      )

      message
  end

  defp validate_message(%Message{batcher: batcher} = message, batchers) do
    if batchers != :none and batcher not in batchers do
      raise "message was set to unknown batcher #{inspect(batcher)}. " <>
              "The known batchers are #{inspect(batchers)}"
    end

    message
  end

  defp validate_message(message, _batchers) do
    raise "expected a Broadway.Message from handle_message/3, got #{inspect(message)}"
  end
end
