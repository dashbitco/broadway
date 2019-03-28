defmodule Broadway.Producer do
  @moduledoc false
  use GenStage
  alias Broadway.Message

  @doc """
  Invoked by the terminator right before Broadway starts draining in-flight
  messages during shutdown.

  This callback should be implemented by producers that need to do additional
  work before shutting down. That includes active producers like RabbitMQ that
  must ask the data provider to stop sending messages.
  """
  @callback prepare_for_draining(state :: any) :: any

  @optional_callbacks prepare_for_draining: 1

  @spec start_link(term, GenServer.options()) :: GenServer.on_start()
  def start_link(args, opts \\ []) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  @spec push_messages(term, [Message.t()]) :: term
  def push_messages(producer, messages) do
    GenStage.call(producer, {:push_messages, messages})
  end

  @impl true
  def init(args) do
    {module, arg} = args[:module]
    transformer = args[:transformer]

    state = %{
      module: module,
      module_state: nil,
      transformer: transformer,
      consumers: []
    }

    case module.init(arg) do
      {:producer, module_state} ->
        {:producer, %{state | module_state: module_state}}

      {:producer, module_state, options} ->
        {:producer, %{state | module_state: module_state}, options}

      return_value ->
        {:stop, {:bad_return_value, return_value}}
    end
  end

  @impl true
  def handle_subscribe(:consumer, _, from, state) do
    {:automatic, update_in(state.consumers, &[from | &1])}
  end

  @impl true
  def handle_cancel(_, from, state) do
    {:noreply, [], update_in(state.consumers, &List.delete(&1, from))}
  end

  @impl true
  def handle_demand(demand, state) do
    %{module: module, transformer: transformer, module_state: module_state} = state

    case module.handle_demand(demand, module_state) do
      {:noreply, events, new_module_state} when is_list(events) ->
        messages = transform_events(events, transformer)
        {:noreply, messages, %{state | module_state: new_module_state}}

      {:noreply, events, new_module_state, :hibernate} ->
        messages = transform_events(events, transformer)
        {:noreply, messages, %{state | module_state: new_module_state}, :hibernate}

      {:stop, reason, new_module_state} ->
        {:stop, reason, %{state | module_state: new_module_state}}
    end
  end

  @impl true
  def handle_cast(:prepare_for_draining, state) do
    %{module: module, module_state: module_state} = state

    if function_exported?(module, :prepare_for_draining, 1) do
      module.prepare_for_draining(module_state)
    end

    {:noreply, [], state}
  end

  @impl true
  def handle_info(:cancel_consumers, state) do
    for from <- state.consumers do
      send(self(), {:"$gen_producer", from, {:cancel, :shutdown}})
    end

    {:noreply, [], state}
  end

  def handle_info(message, state) do
    %{module: module, transformer: transformer, module_state: module_state} = state

    case module.handle_info(message, module_state) do
      {:noreply, events, new_module_state} when is_list(events) ->
        messages = transform_events(events, transformer)
        {:noreply, messages, %{state | module_state: new_module_state}}

      {:noreply, events, new_module_state, :hibernate} ->
        messages = transform_events(events, transformer)
        {:noreply, messages, %{state | module_state: new_module_state}, :hibernate}

      {:stop, reason, new_module_state} ->
        {:stop, reason, %{state | module_state: new_module_state}}
    end
  end

  @impl true
  def handle_call({:push_messages, messages}, _from, state) do
    {:reply, :ok, messages, state}
  end

  @impl true
  def terminate(reason, %{module: module, module_state: module_state}) do
    if function_exported?(module, :terminate, 2) do
      module.terminate(reason, module_state)
    else
      :ok
    end
  end

  defp transform_events(events, nil) do
    case events do
      [] -> :ok
      [message | _] -> validate_message(message)
    end

    events
  end

  defp transform_events(events, {m, f, opts}) do
    for event <- events do
      message = apply(m, f, [event, opts])
      validate_message(message)
    end
  end

  defp validate_message(%Message{} = message) do
    message
  end

  defp validate_message(_message) do
    raise "the produced message is invalid. All messages must be a %Broadway.Message{} " <>
            "struct. In case you're using a standard GenStage producer, please set the " <>
            ":transformer option to transform produced events into message structs"
  end
end
