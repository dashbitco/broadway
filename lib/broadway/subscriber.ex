defmodule Broadway.Subscriber do
  # This modules defines conveniences for subscribing to producers
  # and how to resubscribe to them in case of crashes.
  #
  # In practice, only the first layer resubscribers in case of crashes
  # as the remaining ones are shutdown via the supervision tree which
  # is set as one_for_all and max_restarts of 0 to the inner most
  # supervisor while the outer most is rest for one. This guarantees
  # that either all processess are running or none of them.
  #
  # For graceful shutdowns, we rely on cancellations with the help
  # of the terminator.
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
  def init(names, subscription_options, state, options \\ []) do
    type = Keyword.fetch!(options, :type)
    terminator = Keyword.fetch!(options, :terminator)
    resubscribe = Keyword.fetch!(options, :resubscribe)
    partition = Keyword.fetch!(options, :partition)

    subscription_options =
      subscription_options
      |> Keyword.put(:partition, partition)
      |> Keyword.put_new(:cancel, :temporary)

    state =
      Map.merge(state, %{
        type: type,
        terminator: terminator,
        resubscribe: resubscribe,
        producers: %{},
        consumers: [],
        subscription_options: subscription_options
      })

    names |> Enum.shuffle() |> Enum.each(&subscribe(&1, state))

    if type == :consumer do
      {type, state}
    else
      {type, state, Keyword.take(options, [:dispatcher])}
    end
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

  def handle_info(:cancel_consumers, %{type: :consumer, terminator: terminator} = state) do
    if pid = Process.whereis(terminator) do
      send(pid, {:done, self()})
    end

    {:noreply, [], state}
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

  defp maybe_cancel(%{resubscribe: :never, producers: producers}) when producers == %{} do
    GenStage.async_info(self(), :cancel_consumers)
    true
  end

  defp maybe_cancel(_), do: false
end
