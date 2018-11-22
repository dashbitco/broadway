defmodule Broadway.Consumer do
  use GenStage

  alias Broadway.Subscription

  defmodule State do
    defstruct [:module, :context, :batcher, :batcher_ref, :subscribe_to_options]
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
    subscribe_to_options = [max_demand: 1, min_demand: 0, cancel: :temporary]

    ref = Subscription.subscribe(batcher, subscribe_to_options)

    state = %State{
      module: module,
      context: context,
      batcher: batcher,
      batcher_ref: ref,
      subscribe_to_options: subscribe_to_options
    }

    {:consumer, state}
  end

  def handle_events(events, _from, state) do
    %State{module: module, context: context} = state
    [batch] = events
    %Broadway.Batch{publisher_key: publisher_key} = batch

    {:ack, successful: successful_messages, failed: failed_messages} =
      module.handle_batch(publisher_key, batch, context)

    ack_messages(successful_messages, failed_messages, context)

    {:noreply, [], state}
  end

  def handle_info(:resubscribe, state) do
    %{
      subscribe_to_options: subscribe_to_options,
      batcher: batcher
    } = state

    ref = Subscription.subscribe(batcher, subscribe_to_options)

    {:noreply, [], %{state | batcher_ref: ref}}
  end

  def handle_info({:DOWN, ref, _, _, _reason}, %State{batcher_ref: ref} = state) do
    Subscription.schedule_resubscribe()
    {:noreply, [], %{state | batcher_ref: nil}}
  end

  def handle_info(_, state) do
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
    acknowledger.ack(Enum.reverse(successful), Enum.reverse(failed), context)
  end
end
