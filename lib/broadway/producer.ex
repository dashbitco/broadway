defmodule Broadway.Producer do
  @moduledoc false
  use GenStage
  alias Broadway.Message

  def start_link(args, opts \\ []) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  def push_messages(producer, messages) do
    GenStage.call(producer, {:push_messages, messages})
  end

  @impl true
  def init(args) do
    module = args[:module]
    transformer = args[:transformer]
    # TODO: Raise a proper error message if we don't get  {:producer, state} back.
    {:producer, module_state} = module.init(args[:arg])
    {:producer, %{module: module, module_state: module_state, transformer: transformer}}
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
  def handle_info(:shutdown, state) do
    {:stop, :shutdown, state}
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
