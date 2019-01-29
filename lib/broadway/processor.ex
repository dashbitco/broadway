defmodule Broadway.Processor do
  @moduledoc false
  use GenStage

  alias Broadway.{Message, Acknowledger}

  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(args) do
    processors_config = args[:processors_config]
    context = args[:context]
    partitions = args[:partitions]
    state = %{module: args[:module], context: context}

    subscribe_options =
      Keyword.take(processors_config, [:min_demand, :max_demand]) ++ [cancel: :temporary]

    subscribe_to =
      args[:producers]
      |> Enum.map(&{&1, subscribe_options})

    {:producer_consumer, state,
     subscribe_to: subscribe_to,
     dispatcher: {GenStage.PartitionDispatcher, partitions: partitions, hash: & &1}}
  end

  @impl true
  def handle_events(messages, _from, state) do
    {successful_events, failed_messages} =
      Enum.reduce(messages, {[], []}, fn message, {successful, failed} ->
        %Message{message | processor_pid: self()}
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

  defp classify_returned_message(%Message{publisher: publisher} = message, successful, failed) do
    event = {%Message{message | status: :ok}, publisher}
    {[event | successful], failed}
  end
end
