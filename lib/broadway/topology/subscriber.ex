defmodule Broadway.Topology.Subscriber do
  # This modules defines conveniences for subscribing to producers
  # and how to resubscribe to them in case of crashes.
  #
  # In practice, only the first layer resubscribers in case of crashes
  # as the remaining ones are shutdown via the supervision tree which
  # is set as one_for_all and max_restarts of 0 to the inner most
  # supervisor while the outer most is rest for one. This guarantees
  # that either all processes are running or none of them.
  #
  # For graceful shutdowns, we rely on cancellations with the help
  # of the terminator.
  @moduledoc false
  @behaviour GenStage

  def start_link(module, names, options, subscriptions_options, stage_options) do
    GenStage.start_link(
      __MODULE__,
      {module, names, options, subscriptions_options},
      stage_options
    )
  end

  @impl true
  def init({module, names, options, subscription_options}) do
    {type, state, init_options} = module.init(options)

    terminator = Keyword.fetch!(options, :terminator)
    resubscribe = Keyword.fetch!(options, :resubscribe)
    partition = Keyword.fetch!(options, :partition)

    subscription_options =
      subscription_options
      |> Keyword.put(:partition, partition)
      |> Keyword.put_new(:cancel, :temporary)

    state =
      Map.merge(state, %{
        callback: module,
        terminator: if(type == :consumer, do: terminator),
        resubscribe: resubscribe,
        producers: %{},
        consumers: [],
        subscription_options: subscription_options
      })

    Enum.each(names, &subscribe(&1, state))

    extra_options = if type == :consumer, do: [], else: [buffer_size: :infinity]
    {type, state, extra_options ++ init_options}
  end

  @impl true
  def handle_events(events, from, %{callback: callback} = state) do
    callback.handle_events(events, from, state)
  end

  @impl true
  def handle_subscribe(:producer, opts, {_, ref}, state) do
    process_name = Keyword.fetch!(opts, :name)
    {:automatic, put_in(state.producers[ref], process_name)}
  end

  def handle_subscribe(:consumer, _, from, state) do
    {:automatic, update_in(state.consumers, &[from | &1])}
  end

  @impl true
  def handle_cancel(_, {_, ref} = from, state) do
    case pop_in(state.producers[ref]) do
      {nil, _} ->
        {:noreply, [], update_in(state.consumers, &List.delete(&1, from))}

      {process_name, state} ->
        maybe_resubscribe(process_name, state)
        maybe_cancel(state)
        {:noreply, [], state}
    end
  end

  @impl true
  def handle_info(:will_terminate, state) do
    state = %{state | resubscribe: :never}
    maybe_cancel(state)
    {:noreply, [], state}
  end

  def handle_info(:cancel_consumers, %{terminator: terminator} = state) when terminator != nil do
    if pid = GenServer.whereis(terminator) do
      send(pid, {:done, self()})
    end

    {:noreply, [], state}
  end

  def handle_info(:cancel_consumers, %{callback: callback} = state) do
    case callback.handle_info(:cancel_consumers, state) do
      # If there are no events to emit we are done
      {:noreply, [], state} ->
        for from <- state.consumers do
          send(self(), {:"$gen_producer", from, {:cancel, :shutdown}})
        end

        {:noreply, [], state}

      # Otherwise we will try again later
      other ->
        GenStage.async_info(self(), :cancel_consumers)
        other
    end
  end

  def handle_info({:resubscribe, process_name}, state) do
    subscribe(process_name, state)
    {:noreply, [], state}
  end

  def handle_info(message, %{callback: callback} = state) do
    callback.handle_info(message, state)
  end

  ## Helpers

  defp subscribe(process_name, state) do
    if pid = GenServer.whereis(process_name) do
      opts = [to: pid, name: process_name] ++ state.subscription_options
      GenStage.async_subscribe(self(), opts)
      true
    else
      maybe_resubscribe(process_name, state)
      false
    end
  end

  defp maybe_resubscribe(process_name, %{resubscribe: integer}) when is_integer(integer) do
    Process.send_after(self(), {:resubscribe, process_name}, integer)
    true
  end

  defp maybe_resubscribe(_, _), do: false

  defp maybe_cancel(%{resubscribe: :never, producers: producers}) when producers == %{} do
    GenStage.async_info(self(), :cancel_consumers)
    true
  end

  defp maybe_cancel(_), do: false
end
