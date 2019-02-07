defmodule Broadway.Subscriber do
  # This modules defines conveniences for subscribing to producers
  # and how to resubscribe to them in case of crashes.
  #
  # In practice, only the first layer resubscribers in case of crashes
  # as the remaining ones are shutdown via the supervision tree which
  # is set as one_for_all, with max_restarts of 1. This is done so
  # cancellations model graceful shutdowns exclusively with the help
  # of the terminator.
  #
  # On shutdown:
  #
  #   1. The terminator notifies the first layer that they should no longer resubscribe
  #   2. The terminator tells producers to accumulate demand, flush and shutdown
  #   3. The terminator proceeds to monitor and wait for the termination of the last layer
  #   4. Each layer sees all cancellations from upstream, and cancels downstream via async_info
  #   5. The last layer exits
  #
  # To ensure this is race free:
  #
  #   1. All layers (except the producers) should be one_for_all, permanent, and single max_restarts
  #   2. Therefore, only the first layer (after the producers) resubscribe
  #   3. The upper supervisor should be rest_for_one, permanent, and single max_restarts too
  #   4. The terminator should have a reasonable shutdown child spec timeout
  #
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      @impl true
      defdelegate handle_subscribe(kind, opts, from, state), to: Broadway.Subscriber
      @impl true
      defdelegate handle_cancel(reason, from, state), to: Broadway.Subscriber
      @impl true
      defdelegate handle_info(msg, state), to: Broadway.Subscriber
    end
  end

  @doc """
  Function to be invoked directly on init by users of this module.
  """
  def init(kind, resubscribe, names, subscription_options, state, options \\ [])
      when kind in [:producer_consumer, :consumer] and
             (is_integer(resubscribe) or resubscribe == :never) do
    state =
      Map.merge(state, %{
        kind: kind,
        resubscribe: resubscribe,
        producers: %{},
        consumers: [],
        subscription_options: Keyword.put_new(subscription_options, :cancel, :temporary)
      })

    Enum.each(names, &subscribe(&1, state))
    {kind, state, options}
  end

  ## Callbacks

  def handle_subscribe(:producer, opts, {_, ref}, state) do
    process_name = Keyword.fetch!(opts, :name)
    {:automatic, put_in(state.producers[ref], process_name)}
  end

  def handle_subscribe(:consumer, _, from, state) do
    {:automatic, update_in(state.consumers, &[from | &1])}
  end

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

  def handle_info(:never_resubscribe, state) do
    state = %{state | resubscribe: :never}
    maybe_cancel(state)
    {:noreply, [], state}
  end

  def handle_info(:cancel_consumers, %{kind: :consumer} = state) do
    {:stop, :shutdown, state}
  end

  def handle_info(:cancel_consumers, state) do
    for from <- state.consumers do
      send(self(), {:"$gen_producer", from, {:cancel, :shutdown}})
    end

    {:noreply, [], state}
  end

  def handle_info({:resubscribe, process_name}, state) do
    subscribe(process_name, state)
    {:noreply, [], state}
  end

  def handle_info(_, state) do
    {:noreply, [], state}
  end

  ## Helpers

  defp subscribe(process_name, state) do
    if pid = Process.whereis(process_name) do
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

  defp maybe_cancel(%{resubscribe: :never, producers: producers})
       when producers == %{} do
    GenStage.async_info(self(), :cancel_consumers)
    true
  end

  defp maybe_cancel(_), do: false
end
