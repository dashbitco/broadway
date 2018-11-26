defmodule Broadway.Batcher do
  use GenStage

  alias Broadway.Subscription

  @default_min_demand 2
  @default_max_demand 4

  defmodule State do
    defstruct [
      :batch_size,
      :batch_timeout,
      :publisher_key,
      :pending_events,
      :processors_refs,
      :failed_subscriptions,
      :subscribe_to_options
    ]
  end

  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  def child_spec(args) do
    %{start: {__MODULE__, :start_link, args}}
  end

  def init(args) do
    batch_timeout = Keyword.get(args, :batch_timeout, 1000)
    batch_size = Keyword.get(args, :batch_size, 100)
    publisher_key = Keyword.fetch!(args, :publisher_key)
    min_demand = Keyword.get(args, :min_demand, @default_min_demand)
    max_demand = Keyword.get(args, :max_demand, @default_max_demand)
    processors = Keyword.fetch!(args, :processors)

    subscribe_to_options = [
      partition: publisher_key,
      min_demand: min_demand,
      max_demand: max_demand,
      cancel: :temporary
    ]

    {refs, failed_subscriptions} = Subscription.subscribe_all(processors, subscribe_to_options)

    schedule_flush_pending(batch_timeout)

    {
      :producer_consumer,
      %State{
        publisher_key: publisher_key,
        batch_size: batch_size,
        batch_timeout: batch_timeout,
        pending_events: [],
        processors_refs: refs,
        failed_subscriptions: failed_subscriptions,
        subscribe_to_options: subscribe_to_options
      }
    }
  end

  def handle_events(events, _from, state) do
    %State{pending_events: pending_events, batch_size: batch_size} = state
    do_handle_events(pending_events ++ events, state, batch_size)
  end

  def handle_info(:flush_pending, state) do
    %State{pending_events: pending_events, batch_timeout: batch_timeout} = state
    schedule_flush_pending(batch_timeout)
    do_handle_events(pending_events, state, 1)
  end

  def handle_info(:resubscribe, state) do
    %State{
      processors_refs: processors_refs,
      subscribe_to_options: subscribe_to_options,
      failed_subscriptions: failed_subscriptions
    } = state

    {refs, failed_subscriptions} =
      Subscription.subscribe_all(failed_subscriptions, subscribe_to_options)

    new_state = %State{
      state
      | processors_refs: Map.merge(processors_refs, refs),
        failed_subscriptions: failed_subscriptions
    }

    {:noreply, [], new_state}
  end

  def handle_info({:DOWN, ref, _, _, _reason}, state) do
    %State{
      processors_refs: refs,
      failed_subscriptions: failed_subscriptions
    } = state

    new_state =
      case refs do
        %{^ref => processor} ->
          if Enum.empty?(failed_subscriptions) do
            Subscription.schedule_resubscribe()
          end

          %State{
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
    %State{batch_size: batch_size, publisher_key: publisher_key} = state
    {batch_events, new_pending_events} = split_events(events, publisher_key, batch_size, min_size)

    {:noreply, batch_events, %State{state | pending_events: new_pending_events}}
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
