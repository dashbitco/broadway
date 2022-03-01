defmodule Broadway.Topology.BatcherStage do
  @moduledoc false
  use GenStage
  alias Broadway.BatchInfo

  @all_batches __MODULE__.All

  @spec start_link(term, GenServer.options()) :: GenServer.on_start()
  def start_link(args, stage_options) do
    Broadway.Topology.Subscriber.start_link(
      __MODULE__,
      args[:processors],
      args,
      [max_demand: args[:max_demand] || args[:batch_size]],
      stage_options
    )
  end

  @impl true
  def init(args) do
    Process.put(@all_batches, %{})

    {dispatcher, partition_by} =
      case args[:partition_by] do
        nil ->
          {GenStage.DemandDispatcher, nil}

        func ->
          concurrency = args[:concurrency]
          hash_fun = fn {_, %{partition: partition}} = payload -> {payload, partition} end

          dispatcher =
            {GenStage.PartitionDispatcher, partitions: 0..(concurrency - 1), hash: hash_fun}

          {dispatcher, fn msg -> rem(func.(msg), concurrency) end}
      end

    state = %{
      topology_name: args[:topology_name],
      name: args[:name],
      batcher: args[:batcher],
      batch_size: args[:batch_size],
      batch_timeout: args[:batch_timeout],
      partition_by: partition_by,
      context: args[:context]
    }

    {:producer_consumer, state, dispatcher: dispatcher}
  end

  @impl true
  def handle_events(events, _from, state) do
    batches =
      :telemetry.span(
        [:broadway, :batcher],
        %{
          topology_name: state.topology_name,
          name: state.name,
          batcher_key: state.batcher,
          messages: events,
          context: state.context
        },
        fn ->
          {handle_events_per_batch_key(events, [], state),
           %{
             topology_name: state.topology_name,
             name: state.name,
             batcher_key: state.batcher,
             context: state.context
           }}
        end
      )

    {:noreply, batches, state}
  end

  @impl true
  def handle_info({:timeout, timer, batch_key}, state) do
    case get_timed_out_batch(batch_key, timer) do
      {current, pending_count, _, _} ->
        delete_batch(batch_key)
        {:noreply, [wrap_for_delivery(batch_key, current, pending_count, state)], state}

      :error ->
        {:noreply, [], state}
    end
  end

  def handle_info(:cancel_consumers, state) do
    events =
      for {batch_key, _} <- all_batches() do
        {current, pending_count, _, timer} = delete_batch(batch_key)
        cancel_batch_timeout(timer)
        wrap_for_delivery(batch_key, current, pending_count, state)
      end

    {:noreply, events, state}
  end

  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  ## Default batch handling

  defp handle_events_per_batch_key([], acc, _state) do
    Enum.reverse(acc)
  end

  defp handle_events_per_batch_key([event | _] = events, acc, state) do
    %{partition_by: partition_by} = state
    batch_key = batch_key(event, partition_by)
    {current, pending_count, batch_splitter, timer} = init_or_get_batch(batch_key, state)

    {current, pending_count, events, flush?} =
      split_counting(
        batch_key,
        events,
        pending_count,
        batch_splitter,
        false,
        current,
        partition_by
      )

    acc =
      deliver_or_update_batch(
        batch_key,
        current,
        pending_count,
        batch_splitter,
        flush?,
        timer,
        acc,
        state
      )

    handle_events_per_batch_key(events, acc, state)
  end

  defp split_counting(_batch_key, [], count, _batch_splitter, flush?, acc, _partition_by) do
    {acc, count, [], flush?}
  end

  defp split_counting(
         batch_key,
         [event | remained] = events,
         count,
         batch_splitter,
         flush?,
         acc,
         partition_by
       ) do
    event_batch_key = batch_key(event, partition_by)
    remained_count = batch_splitter.(event, count)

    cond do
      event_batch_key != batch_key ->
        # Switch to a different batch key
        {acc, count, events, flush?}

      remained_count == :done ->
        # Complete one batch, reset the count = nil so that splitter can count again
        {[event | acc], nil, remained, flush?}

      true ->
        # Same batch key but not fulfill one batch size yet
        flush? = flush? or event.batch_mode == :flush

        split_counting(
          batch_key,
          remained,
          remained_count,
          batch_splitter,
          flush?,
          [event | acc],
          partition_by
        )
    end
  end

  defp deliver_or_update_batch(
         batch_key,
         current,
         pending_count,
         _batch_splitter,
         true,
         timer,
         acc,
         state
       ) do
    deliver_batch(batch_key, current, pending_count, timer, acc, state)
  end

  defp deliver_or_update_batch(
         batch_key,
         current,
         nil,
         _batch_splitter,
         _flush?,
         timer,
         acc,
         state
       ) do
    deliver_batch(batch_key, current, :done, timer, acc, state)
  end

  defp deliver_or_update_batch(
         batch_key,
         current,
         pending_count,
         batch_splitter,
         _flush?,
         timer,
         acc,
         _state
       ) do
    put_batch(batch_key, {current, pending_count, batch_splitter, timer})
    acc
  end

  defp deliver_batch(batch_key, current, pending_count, timer, acc, state) do
    delete_batch(batch_key)
    cancel_batch_timeout(timer)
    [wrap_for_delivery(batch_key, current, pending_count, state) | acc]
  end

  ## General batch handling

  @compile {:inline, batch_key: 2}

  defp batch_key(%{batch_key: batch_key}, nil),
    do: batch_key

  defp batch_key(%{batch_key: batch_key} = event, partition_by),
    do: [batch_key | partition_by.(event)]

  defp init_or_get_batch(batch_key, state) do
    if batch = Process.get(batch_key) do
      batch
    else
      %{batch_size: batch_size, batch_timeout: batch_timeout} = state

      batch_splitter = get_batch_splitter(batch_size)
      timer = schedule_batch_timeout(batch_key, batch_timeout)
      update_all_batches(&Map.put(&1, batch_key, true))
      {[], nil, batch_splitter, timer}
    end
  end

  defp get_batch_splitter(batch_size) do
    if is_number(batch_size) do
      fn _message, remained ->
        # Initialize batch split counter
        remained = if(remained == nil, do: batch_size - 1, else: remained - 1)
        if(remained == 0, do: :done, else: remained)
      end
    else
      # Customized batch splitter function should have default batch size on first call
      batch_size
    end
  end

  defp get_timed_out_batch(batch_key, timer) do
    case Process.get(batch_key) do
      {_, _, _, ^timer} = batch -> batch
      _ -> :error
    end
  end

  defp put_batch(batch_key, {_, _, _, _} = batch) do
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

  defp wrap_for_delivery(batch_key, reversed_events, pending, %{partition_by: nil} = state) do
    wrap_for_delivery(batch_key, nil, reversed_events, pending, state)
  end

  defp wrap_for_delivery([batch_key | partition], reversed_events, pending, state) do
    wrap_for_delivery(batch_key, partition, reversed_events, pending, state)
  end

  defp wrap_for_delivery(batch_key, partition, reversed_events, pending, state) do
    %{batcher: batcher} = state
    [event | _] = reversed_events

    trigger =
      case event.batch_mode do
        :bulk -> if(pending == :done, do: :size, else: :timeout)
        :flush -> :flush
      end

    batch_info = %BatchInfo{
      batcher: batcher,
      batch_key: batch_key,
      partition: partition,
      size: length(reversed_events),
      trigger: trigger
    }

    {Enum.reverse(reversed_events), batch_info}
  end
end
