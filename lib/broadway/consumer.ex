defmodule Broadway.Consumer do
  @moduledoc false
  use GenStage

  alias Broadway.Subscription
  @subscribe_to_options [max_demand: 1, min_demand: 0, cancel: :temporary]

  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(args) do
    batcher = args[:batcher]
    ref = Subscription.subscribe(batcher, @subscribe_to_options)

    state = %{
      module: args[:module],
      context: args[:context],
      batcher: batcher,
      batcher_ref: ref
    }

    {:consumer, state}
  end

  @impl true
  def handle_events(events, _from, state) do
    %{module: module, context: context} = state
    [{messages, batch_info}] = events
    %Broadway.BatchInfo{publisher_key: publisher_key} = batch_info

    # TODO: Raise a proper error message if handle_batch
    # does not return {:ack, ...} (within the upcoming try/catch).
    {:ack, successful: successful_messages, failed: failed_messages} =
      module.handle_batch(publisher_key, messages, batch_info, context)

    ack_messages(successful_messages, failed_messages)

    {:noreply, [], state}
  end

  @impl true
  def handle_info(:resubscribe, state) do
    %{batcher: batcher} = state
    ref = Subscription.subscribe(batcher, @subscribe_to_options)
    {:noreply, [], %{state | batcher_ref: ref}}
  end

  def handle_info({:DOWN, ref, _, _, _reason}, %{batcher_ref: ref} = state) do
    Subscription.schedule_resubscribe()
    {:noreply, [], %{state | batcher_ref: nil}}
  end

  def handle_info(_, state) do
    {:noreply, [], state}
  end

  defp ack_messages(successful_messages, failed_messages) do
    %{}
    |> group_by_acknowledger(successful_messages, :successful)
    |> group_by_acknowledger(failed_messages, :failed)
    |> Enum.each(&call_ack/1)
  end

  defp group_by_acknowledger(grouped_messages, messages, key) do
    Enum.reduce(messages, grouped_messages, fn %{acknowledger: {acknowledger, _}} = msg, acc ->
      Map.update(acc, acknowledger, [{key, msg}], &[{key, msg} | &1])
    end)
  end

  defp call_ack({acknowledger, messages}) do
    {successful, failed} = unpack_messages(messages, [], [])
    acknowledger.ack(successful, failed)
  end

  defp unpack_messages([{:successful, message} | messages], successful, failed),
    do: unpack_messages(messages, [message | successful], failed)

  defp unpack_messages([{:failed, message} | messages], successful, failed),
    do: unpack_messages(messages, successful, [message | failed])

  defp unpack_messages([], successful, failed),
    do: {successful, failed}
end
