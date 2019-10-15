defmodule Broadway.Batcher do
  @moduledoc false
  use GenStage
  use Broadway.Subscriber
  alias Broadway.BatchInfo

  @all_batches __MODULE__.All

  @spec start_link(term, GenServer.options()) :: GenServer.on_start()
  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(args) do
    Process.put(@all_batches, %{})
    batcher = args[:batcher]
    partition_by = args[:partition_by]

    state = %{
      batcher: batcher,
      batch_size: args[:batch_size],
      batch_timeout: args[:batch_timeout]
    }

    dispatcher =
      case partition_by do
        nil ->
          GenStage.DemandDispatcher

        func ->
          stages = args[:stages]

          hash_func = fn {[msg | _], _info} = payload ->
            {payload, rem(func.(msg), stages)}
          end

          {GenStage.PartitionDispatcher, partitions: 0..(stages - 1), hash: hash_func}
      end

    Broadway.Subscriber.init(
      args[:processors],
      [max_demand: args[:batch_size]],
      state,
      [dispatcher: dispatcher] ++ args
    )
  end

  @impl true
  def handle_events(events, _from, state) do
    batches = handle_events_per_batch_key(events, [], state)
    {:noreply, batches, state}
  end

  defoverridable handle_info: 2

  @impl true
  def handle_info({:timeout, timer, batch_key}, state) do
    case get_timed_out_batch(batch_key, timer) do
      {current, _, _} ->
        delete_batch(batch_key)
        {:noreply, [wrap_for_delivery(batch_key, current, state)], state}

      :error ->
        {:noreply, [], state}
    end
  end

  # Hijack subscriber events to publish batches
  def handle_info(:cancel_consumers, state) do
    batches = all_batches()

    if batches == %{} do
      super(:cancel_consumers, state)
    else
      events =
        for {batch_key, _} <- batches do
          {current, _, timer} = delete_batch(batch_key)
          cancel_batch_timeout(timer)
          wrap_for_delivery(batch_key, current, state)
        end

      GenStage.async_info(self(), :cancel_consumers)
      {:noreply, events, state}
    end
  end

  def handle_info(msg, state) do
    super(msg, state)
  end

  ## Default batch handling

  defp handle_events_per_batch_key([], acc, _state) do
    Enum.reverse(acc)
  end

  defp handle_events_per_batch_key([%{batch_key: batch_key} | _] = events, acc, state) do
    {current, pending_count, timer} = init_or_get_batch(batch_key, state)

    {current, pending_count, events, flush?} =
      split_counting(batch_key, events, pending_count, false, current)

    acc = deliver_or_update_batch(batch_key, current, pending_count, flush?, timer, acc, state)
    handle_events_per_batch_key(events, acc, state)
  end

  defp split_counting(batch_key, [%{batch_key: batch_key} = event | events], count, flush?, acc)
       when count > 0 do
    flush? = flush? or event.batch_mode == :flush
    split_counting(batch_key, events, count - 1, flush?, [event | acc])
  end

  defp split_counting(_batch_key, events, count, flush?, acc), do: {acc, count, events, flush?}

  defp deliver_or_update_batch(batch_key, current, _pending_count, true, timer, acc, state) do
    deliver_batch(batch_key, current, timer, acc, state)
  end

  defp deliver_or_update_batch(batch_key, current, 0, _flush?, timer, acc, state) do
    deliver_batch(batch_key, current, timer, acc, state)
  end

  defp deliver_or_update_batch(batch_key, current, pending_count, _flush?, timer, acc, _state) do
    put_batch(batch_key, {current, pending_count, timer})
    acc
  end

  defp deliver_batch(batch_key, current, timer, acc, state) do
    delete_batch(batch_key)
    cancel_batch_timeout(timer)
    [wrap_for_delivery(batch_key, current, state) | acc]
  end

  ## General batch handling

  defp init_or_get_batch(batch_key, state) do
    if batch = Process.get(batch_key) do
      batch
    else
      %{batch_size: batch_size, batch_timeout: batch_timeout} = state
      timer = schedule_batch_timeout(batch_key, batch_timeout)
      update_all_batches(&Map.put(&1, batch_key, true))
      {[], batch_size, timer}
    end
  end

  defp get_timed_out_batch(batch_key, timer) do
    case Process.get(batch_key) do
      {_, _, ^timer} = batch -> batch
      _ -> :error
    end
  end

  defp put_batch(batch_key, {_, _, _} = batch) do
    Process.put(batch_key, batch)
  end

  defp delete_batch(batch_key) do
    update_all_batches(&Map.delete(&1, batch_key))
    Process.delete(batch_key)
  end

  defp all_batches do
    Process.get(@all_batches)
  end

  defp update_all_batches(fun) do
    Process.put(@all_batches, fun.(Process.get(@all_batches)))
  end

  defp schedule_batch_timeout(batch_key, batch_timeout) do
    :erlang.start_timer(batch_timeout, self(), batch_key)
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

  defp wrap_for_delivery(batch_key, reversed_events, state) do
    %{batcher: batcher} = state

    batch_info = %BatchInfo{
      batcher: batcher,
      batch_key: batch_key
    }

    {Enum.reverse(reversed_events), batch_info}
  end
end
