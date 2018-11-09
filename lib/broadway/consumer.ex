defmodule Broadway.Consumer do
  use GenStage

  defmodule State do
    defstruct [:module, :context]
  end

  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  def child_spec(args) do
    %{start: {__MODULE__, :start_link, args}}
  end

  def init(args) do
    module = Keyword.fetch!(args, :module)
    context = Keyword.fetch!(args, :context)
    batcher = Keyword.fetch!(args, :batcher)
    subscribe_to = [{batcher, max_demand: 2, min_demand: 1}]

    {:consumer, %State{module: module, context: context}, subscribe_to: subscribe_to}
  end

  def handle_events(events, _from, state) do
    %State{module: module, context: context} = state
    [batch] = events
    %Broadway.Batch{partition: partition} = batch

    {:ack, successful: successful_messages, failed: failed_messages} =
      module.handle_batch(partition, batch, context)

    ack_messages(successful_messages, failed_messages, context)

    {:noreply, [], state}
  end

  defp ack_messages(successful_messages, failed_messages, context) do
    %{}
    |> reduce_messages_grouping_by_acknowledger(successful_messages, :successful)
    |> reduce_messages_grouping_by_acknowledger(failed_messages, :failed)
    |> Enum.each(&call_ack(&1, context))
  end

  defp reduce_messages_grouping_by_acknowledger(grouped_messages, messages, key) do
    Enum.reduce(messages, grouped_messages, fn msg, acc ->
      add_message_to_acknowledger(msg, acc, key)
    end)
  end

  defp add_message_to_acknowledger(%{acknowledger: {acknowledger, _}} = msg, acc, key) do
    acc
    |> Map.get(acknowledger, %{successful: [], failed: []})
    |> Map.update!(key, &[msg | &1])
    |> (&Map.put(acc, acknowledger, &1)).()
  end

  defp call_ack({acknowledger, %{successful: successful, failed: failed}}, context) do
    acknowledger.ack(successful, failed, context)
  end
end
