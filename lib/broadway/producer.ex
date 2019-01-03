defmodule Broadway.Producer do
  @moduledoc false
  use GenStage

  def start_link(args, opts \\ []) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  def push_messages(producer, messages) do
    GenStage.call(producer, {:push_messages, messages})
  end

  @impl true
  def init(args) do
    module = args[:module]
    # TODO: Raise a proper error message if we don't {:producer, state} back.
    {:producer, module_state} = module.init(args[:args])
    {:producer, %{module: module, module_state: module_state}}
  end

  @impl true
  def handle_demand(demand, %{module: module, module_state: module_state} = state) do
    # TODO: Raise a proper error message if we don't get a list of
    # messages as events and point people towards the upcoming transform.
    case module.handle_demand(demand, module_state) do
      {tag, events_or_reason, new_module_state} ->
        {tag, events_or_reason, %{state | module_state: new_module_state}}

      {:noreply, events, new_module_state, :hibernate} ->
        {:noreply, events, %{state | module_state: new_module_state}, :hibernate}
    end
  end

  @impl true
  def handle_info(message, %{module: module, module_state: module_state} = state) do
    case module.handle_info(message, module_state) do
      {tag, events_or_reason, new_module_state} ->
        {tag, events_or_reason, %{state | module_state: new_module_state}}

      {:noreply, events, new_module_state, :hibernate} ->
        {:noreply, events, %{state | module_state: new_module_state}, :hibernate}
    end
  end

  @impl true
  def handle_call({:push_messages, messages}, _from, state) do
    {:reply, :ok, messages, state}
  end

  @impl true
  def terminate(reason, %{module: module, module_state: module_state}) do
    module.terminate(reason, module_state)
  end
end
