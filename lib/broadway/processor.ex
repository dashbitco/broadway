defmodule Broadway.Processor do
  @moduledoc false
  use GenStage
  use Broadway.Subscriber

  alias Broadway.{Message, Acknowledger}

  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(args) do
    processor_config = args[:processor_config]
    processor_key = args[:processor_key]
    context = args[:context]
    state = %{module: args[:module], context: context, processor_key: processor_key}

    Broadway.Subscriber.init(
      args[:producers],
      Keyword.take(processor_config, [:min_demand, :max_demand]),
      state,
      args
    )
  end

  @impl true
  def handle_events(messages, _from, state) do
    {successful_events, failed_messages} =
      Enum.reduce(messages, {[], []}, fn message, {successful, failed} ->
        message
        |> handle_message(state)
        |> classify_returned_message(successful, failed)
      end)

    Acknowledger.ack_messages([], Enum.reverse(failed_messages))
    {:noreply, Enum.reverse(successful_events), state}
  end

  defp handle_message(message, state) do
    %{module: module, context: context, processor_key: processor_key} = state

    try do
      module.handle_message(processor_key, message, context)
    rescue
      e ->
        error_message = Exception.message(e)
        Message.failed(message, error_message)
    end
  end

  defp classify_returned_message(%Message{status: {:failed, _}} = message, successful, failed) do
    {successful, [message | failed]}
  end

  defp classify_returned_message(%Message{} = message, successful, failed) do
    {[message | successful], failed}
  end
end
