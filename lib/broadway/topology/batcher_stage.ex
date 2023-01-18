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
  def handle_info({:timeout, _timer, ref}, state) do
    case all_batches() do
      %{^ref => batch_key} ->
        {current, _, _, _, _} = delete_batch(batch_key, ref)
        {:noreply, [wrap_for_delivery(batch_key, current, :timeout, state)], state}

      %{} ->
        {:noreply, [], state}
    end
  end

  def handle_info(:cancel_consumers, state) do
    events =
      for {ref, batch_key} <- all_batches() do
        {current, _, _, timer, _} = delete_batch(batch_key, ref)
        cancel_batch_timeout(timer)
        wrap_for_delivery(batch_key, current, :flush, state)
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
    {current, batch_state, batch_splitter, timer, ref} = init_or_get_batch(batch_key, state)

    {current, batch_state, events, flush} =
      split_counting(
        batch_key,
        events,
        batch_state,
        batch_splitter,
        nil,
        current,
        partition_by
      )

    acc =
      if flush do
        deliver_batch(batch_key, current, flush, timer, ref, acc, state)
      else
        put_batch(batch_key, {current, batch_state, batch_splitter, timer, ref})
        acc
      end

    handle_events_per_batch_key(events, acc, state)
  end

  defp split_counting(_batch_key, [], count, _batch_splitter, flush?, acc, _partition_by) do
    {acc, count, [], flush?}
  end

  defp split_counting(
         batch_key,
         [event | remained] = events,
         batch_state,
         batch_splitter,
         flush,
         acc,
         partition_by
       ) do
    event_batch_key = batch_key(event, partition_by)

    # Switch to a different batch key
    if event_batch_key != batch_key do
      {acc, batch_state, events, flush}
    else
      case batch_splitter.(event, batch_state) do
        # Batch splitter indicates a full batch
        {:emit, next_state} ->
          {[event | acc], next_state, remained, :size}

        # Same batch key but not fulfill one batch size yet
        {:cont, next_state} ->
          split_counting(
            batch_key,
            remained,
            next_state,
            batch_splitter,
            flush || flush_batch(event),
            [event | acc],
            partition_by
          )
      end
    end
  end

  defp flush_batch(%{batch_mode: :flush}), do: :flush
  defp flush_batch(%{}), do: nil

  defp deliver_batch(batch_key, current, trigger, timer, ref, acc, state) do
    delete_batch(batch_key, ref)
    cancel_batch_timeout(timer)
    [wrap_for_delivery(batch_key, current, trigger, state) | acc]
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

      {batch_state, batch_splitter} = get_batch_splitter(batch_size)
      {timer, ref} = schedule_batch_timeout(batch_timeout)
      update_all_batches(&Map.put(&1, ref, batch_key))
      {[], batch_state, batch_splitter, timer, ref}
    end
  end

  defp get_batch_splitter(batch_size) do
    if is_number(batch_size) do
      {batch_size,
       fn
         _message, 1 -> {:emit, batch_size}
         _message, count -> {:cont, count - 1}
       end}
    else
      # Customized batch splitter with initial state and function
      batch_size
    end
  end

  defp put_batch(batch_key, {_, _, _, _, _} = batch) do
    Process.put(batch_key, batch)
  end

  defp delete_batch(batch_key, ref) do
    update_all_batches(&Map.delete(&1, ref))
    Process.delete(batch_key)
  end

  defp all_batches do
    Process.get(@all_batches)
  end

  defp update_all_batches(fun) do
    Process.put(@all_batches, fun.(Process.get(@all_batches)))
  end

  defp schedule_batch_timeout(batch_timeout) do
    ref = make_ref()
    {:erlang.start_timer(batch_timeout, self(), ref), ref}
  end

  defp cancel_batch_timeout(timer) do
    case :erlang.cancel_timer(timer) do
      false ->
        receive do
          {:timeout, ^timer, _} -> :ok
        after
          0 -> :ok
        end

      _ ->
        :ok
    end
  end

  defp wrap_for_delivery(batch_key, reversed_events, trigger, %{partition_by: nil} = state) do
    wrap_for_delivery(batch_key, nil, reversed_events, trigger, state)
  end

  defp wrap_for_delivery([batch_key | partition], reversed_events, trigger, state) do
    wrap_for_delivery(batch_key, partition, reversed_events, trigger, state)
  end

  defp wrap_for_delivery(batch_key, partition, reversed_events, trigger, state) do
    %{batcher: batcher} = state

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
