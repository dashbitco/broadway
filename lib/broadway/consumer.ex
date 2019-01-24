defmodule Broadway.Consumer do
  @moduledoc false
  use GenStage

  alias Broadway.{Subscription, Acknowledger}
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

    Acknowledger.ack_messages(successful_messages, failed_messages)

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
end
