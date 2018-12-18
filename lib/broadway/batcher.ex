defmodule Broadway.Batcher do
  @moduledoc false
  use GenStage
  alias Broadway.Subscription

  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(args) do
    publisher_key = args[:publisher_key]
    batch_timeout = args[:batch_timeout]

    subscribe_to_options = [
      partition: publisher_key,
      max_demand: args[:batch_size],
      cancel: :temporary
    ]

    {refs, failed_subscriptions} =
      Subscription.subscribe_all(args[:processors], subscribe_to_options)

    # TODO: We should probably do this when we add the first element
    # to the batch.
    schedule_flush_pending(batch_timeout)

    state = %{
      publisher_key: publisher_key,
      batch_size: args[:batch_size],
      batch_timeout: batch_timeout,
      pending_events: [],
      processors_refs: refs,
      failed_subscriptions: failed_subscriptions,
      subscribe_to_options: subscribe_to_options
    }

    {:producer_consumer, state}
  end

  @impl true
  def handle_events(events, _from, state) do
    # TODO: This could be made more efficient. We should store
    # in the state how many elements we already have in the current
    # batch (or how many is left to fullfil the batch). Then when
    # new events come, we just need to take whatever is left to
    # compute the batch, without computing the whole batch again.
    # Another suggestion, use the process dictionary for efficiency.
    %{pending_events: pending_events, batch_size: batch_size} = state
    do_handle_events(pending_events ++ events, state, batch_size)
  end

  @impl true
  def handle_info(:flush_pending, state) do
    %{pending_events: pending_events, batch_timeout: batch_timeout} = state
    schedule_flush_pending(batch_timeout)
    do_handle_events(pending_events, state, 1)
  end

  def handle_info(:resubscribe, state) do
    %{
      processors_refs: processors_refs,
      subscribe_to_options: subscribe_to_options,
      failed_subscriptions: failed_subscriptions
    } = state

    {refs, failed_subscriptions} =
      Subscription.subscribe_all(failed_subscriptions, subscribe_to_options)

    new_state = %{
      state
      | processors_refs: Map.merge(processors_refs, refs),
        failed_subscriptions: failed_subscriptions
    }

    {:noreply, [], new_state}
  end

  def handle_info({:DOWN, ref, _, _, _reason}, state) do
    %{
      processors_refs: refs,
      failed_subscriptions: failed_subscriptions
    } = state

    new_state =
      case refs do
        %{^ref => processor} ->
          if Enum.empty?(failed_subscriptions) do
            Subscription.schedule_resubscribe()
          end

          %{
            state
            | processors_refs: Map.delete(refs, ref),
              failed_subscriptions: [processor | failed_subscriptions]
          }

        _ ->
          state
      end

    {:noreply, [], new_state}
  end

  def handle_info(_, state) do
    {:noreply, [], state}
  end

  defp do_handle_events(events, state, min_size) do
    %{batch_size: batch_size, publisher_key: publisher_key} = state
    {batch_events, new_pending_events} = split_events(events, publisher_key, batch_size, min_size)

    {:noreply, batch_events, %{state | pending_events: new_pending_events}}
  end

  defp split_events(events, publisher_key, batch_size, min_size) do
    {batch_events, pending_events} = Enum.split(events, batch_size)

    if length(batch_events) >= min_size do
      {[{batch_events, %Broadway.BatchInfo{publisher_key: publisher_key, batcher: self()}}],
       pending_events}
    else
      {[], events}
    end
  end

  defp schedule_flush_pending(delay) do
    Process.send_after(self(), :flush_pending, delay)
  end
end
