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
    processors_config = args[:processors_config]
    context = args[:context]
    state = %{module: args[:module], context: context}

    Broadway.Subscriber.init(
      args[:producers],
      Keyword.take(processors_config, [:min_demand, :max_demand]),
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
    %{module: module, context: context} = state

    try do
      module.handle_message(message, context)
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
