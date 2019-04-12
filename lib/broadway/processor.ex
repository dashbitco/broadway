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
      partitions: args[:partitions]
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
      partitions: partitions
    } = state

    try do
      module.handle_message(processor_key, message, context)
      |> validate_message(partitions)
    catch
      kind, error ->
        Logger.error(Exception.format(kind, error, System.stacktrace()))
        message = Message.failed(message, "due to an unhandled #{kind}")
        handle_messages(messages, successful, [message | failed], state)
    else
      %{status: {:failed, _}} = message ->
        handle_messages(messages, successful, [message | failed], state)

      message ->
        handle_messages(messages, [message | successful], failed, state)
    end
  end

  defp handle_messages([], successful, failed, _state) do
    {Enum.reverse(successful), Enum.reverse(failed)}
  end

  defp validate_message(%Message{} = message, []) do
    message
  end

  defp validate_message(%Message{batcher: batcher} = message, partitions) do
    if batcher not in partitions do
      raise "message was set to unknown batcher #{inspect(batcher)}. " <>
              "The known batchers are #{inspect(partitions)}"
    end

    message
  end

  defp validate_message(message, _partitions) do
    raise "expected a Broadway.Message from handle_message/3, got #{inspect(message)}"
  end
end
