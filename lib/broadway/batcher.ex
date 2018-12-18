defmodule Broadway.Batcher do
  @moduledoc false
  use GenStage
  alias Broadway.{BatchInfo, Subscription}

  @default_batch __MODULE__

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

    state = %{
      publisher_key: publisher_key,
      batch_size: args[:batch_size],
      batch_timeout: batch_timeout,
      processors_refs: refs,
      failed_subscriptions: failed_subscriptions,
      subscribe_to_options: subscribe_to_options
    }

    {:producer_consumer, state}
  end

  @impl true
  def handle_events(events, _from, state) do
    batches = handle_events_for_default_batch(events, [], state)
    {:noreply, batches, state}
  end

  @impl true
  def handle_info({:timeout, timer, batch_name}, state) do
    case get_timed_out_batch(batch_name, timer) do
      {current, _, _} ->
        delete_batch(batch_name)
        {:noreply, [wrap_for_delivery(current, state)], state}

      :error ->
        {:noreply, [], state}
    end
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

  ## Default batch handling

  defp handle_events_for_default_batch([], acc, _state) do
    Enum.reverse(acc)
  end

  defp handle_events_for_default_batch(events, acc, state) do
    {current, pending_count, timer} = init_or_get_batch(@default_batch, state)
    {current, pending_count, events} = split_counting(events, pending_count, current)

    if pending_count == 0 do
      delete_batch(@default_batch)
      cancel_batch_timeout(timer)
      handle_events_for_default_batch(events, [wrap_for_delivery(current, state) | acc], state)
    else
      put_batch(@default_batch, {current, pending_count, timer})
      handle_events_for_default_batch(events, acc, state)
    end
  end

  defp split_counting([event | events], count, acc) when count > 0 do
    split_counting(events, count - 1, [event | acc])
  end

  defp split_counting(events, count, acc), do: {acc, count, events}

  ## General batch handling

  defp init_or_get_batch(batch_name, state) do
    if batch = Process.get(batch_name) do
      batch
    else
      %{batch_size: batch_size, batch_timeout: batch_timeout} = state
      timer = schedule_batch_timeout(batch_name, batch_timeout)
      {[], batch_size, timer}
    end
  end

  defp get_timed_out_batch(batch_name, timer) do
    case Process.get(batch_name) do
      {_, _, ^timer} = batch -> batch
      _ -> :error
    end
  end

  defp put_batch(batch_name, {_, _, _} = batch) do
    Process.put(batch_name, batch)
  end

  defp delete_batch(batch_name) do
    Process.delete(batch_name)
  end

  defp schedule_batch_timeout(batch_name, batch_timeout) do
    :erlang.start_timer(batch_timeout, self(), batch_name)
  end

  defp cancel_batch_timeout(timer) do
    case :erlang.cancel_timer(timer) do
      false ->
        receive do
          {:timeout, ^timer, _} -> :ok
        after
          0 -> raise "unknown timer #{inspect(timer)}"
        end

      _ ->
        :ok
    end
  end

  defp wrap_for_delivery(reversed_events, state) do
    %{publisher_key: publisher_key} = state
    batch_info = %BatchInfo{publisher_key: publisher_key, batcher: self()}
    {Enum.reverse(reversed_events), batch_info}
  end
end
