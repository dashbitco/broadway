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
    batcher_key = args[:batcher_key]

    state = %{
      batcher_key: batcher_key,
      batch_size: args[:batch_size],
      batch_timeout: args[:batch_timeout]
    }

    Broadway.Subscriber.init(
      args[:processors],
      [partition: batcher_key, max_demand: args[:batch_size]],
      state,
      args
    )
  end

  @impl true
  def handle_events(events, _from, state) do
    batches = handle_events_per_partition(events, [], state)
    {:noreply, batches, state}
  end

  defoverridable handle_info: 2

  @impl true
  def handle_info({:timeout, timer, partition}, state) do
    case get_timed_out_batch(partition, timer) do
      {current, _, _} ->
        delete_batch(partition)
        {:noreply, [wrap_for_delivery(partition, current, state)], state}

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
        for {partition, _} <- batches do
          {current, _, timer} = delete_batch(partition)
          cancel_batch_timeout(timer)
          wrap_for_delivery(partition, current, state)
        end

      GenStage.async_info(self(), :cancel_consumers)
      {:noreply, events, state}
    end
  end

  def handle_info(msg, state) do
    super(msg, state)
  end

  ## Default batch handling

  defp handle_events_per_partition([], acc, _state) do
    Enum.reverse(acc)
  end

  defp handle_events_per_partition([%{partition: partition} | _] = events, acc, state) do
    {current, pending_count, timer} = init_or_get_batch(partition, state)
    {current, pending_count, events} = split_counting(partition, events, pending_count, current)

    acc = deliver_or_update_batch(partition, current, pending_count, timer, acc, state)
    handle_events_per_partition(events, acc, state)
  end

  defp split_counting(partition, [%{partition: partition} = event | events], count, acc)
       when count > 0,
       do: split_counting(events, count - 1, [event | acc])

  defp split_counting(events, count, acc), do: {acc, count, events}

  defp deliver_or_update_batch(partition, current, 0, timer, acc, state) do
    delete_batch(partition)
    cancel_batch_timeout(timer)
    [wrap_for_delivery(partition, current, state) | acc]
  end

  defp deliver_or_update_batch(partition, current, pending_count, timer, acc, _state) do
    put_batch(partition, {current, pending_count, timer})
    acc
  end

  ## General batch handling

  defp init_or_get_batch(batch_name, state) do
    if batch = Process.get(batch_name) do
      batch
    else
      %{batch_size: batch_size, batch_timeout: batch_timeout} = state
      timer = schedule_batch_timeout(batch_name, batch_timeout)
      update_all_batches(&Map.put(&1, batch_name, true))
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
    update_all_batches(&Map.delete(&1, batch_name))
    Process.delete(batch_name)
  end

  defp all_batches do
    Process.get(@all_batches)
  end

  defp update_all_batches(fun) do
    Process.put(@all_batches, fun.(Process.get(@all_batches)))
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

  defp wrap_for_delivery(partition, reversed_events, state) do
    %{batcher_key: batcher_key} = state

    batch_info = %BatchInfo{
      batcher_key: batcher_key,
      batcher_pid: self(),
      partition: partition
    }

    {Enum.reverse(reversed_events), batch_info}
  end
end
