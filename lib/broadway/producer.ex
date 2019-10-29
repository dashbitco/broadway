defmodule Broadway.Producer do
  @moduledoc false
  use GenStage
  alias Broadway.Message

  @doc """
  Invoked once by Broadway during `Broadway.start_link/2`.

  The goal of this task is to manipulate the general topology options,
  if necessary at all, and introduce any new child specs that will be
  started before the ProducerSupervisor in Broadwday's supervision tree.

  It is guaranteed to be invoked inside the Broadway main process.

  The options include all of Broadway topology options.
  """
  @callback prepare_for_start(module :: atom, options :: keyword) ::
              {[:supervisor.child_spec() | {module, any} | module], options :: keyword}

  @doc """
  Invoked by the terminator right before Broadway starts draining in-flight
  messages during shutdown.

  This callback should be implemented by producers that need to do additional
  work before shutting down. That includes active producers like RabbitMQ that
  must ask the data provider to stop sending messages. It will be invoked for
  each producer stage.
  """
  @callback prepare_for_draining(state :: any) :: any

  @optional_callbacks prepare_for_start: 2, prepare_for_draining: 1

  @spec start_link(term, GenServer.options()) :: GenServer.on_start()
  def start_link(args, index, opts \\ []) do
    GenStage.start_link(__MODULE__, {args, index}, opts)
  end

  @spec push_messages(GenServer.server(), [Message.t()]) :: :ok
  def push_messages(producer, messages) do
    GenStage.call(producer, {__MODULE__, :push_messages, messages})
  end

  @spec drain(GenServer.server()) :: :ok
  def drain(producer) do
    GenStage.cast(producer, {__MODULE__, :prepare_for_draining})
    GenStage.demand(producer, :accumulate)
    GenStage.async_info(producer, {__MODULE__, :cancel_consumers})
  end

  @impl true
  def init({args, index}) do
    {module, arg} = args[:module]
    transformer = args[:transformer]
    dispatcher = args[:dispatcher]

    # Inject the topology index only if the args are a keyword list.
    arg =
      if Keyword.keyword?(arg) do
        Keyword.put(arg, :broadway, Keyword.put(args[:broadway], :index, index))
      else
        arg
      end

    state = %{
      module: module,
      module_state: nil,
      transformer: transformer,
      consumers: []
    }

    case module.init(arg) do
      {:producer, module_state} ->
        {:producer, %{state | module_state: module_state}, dispatcher: dispatcher}

      {:producer, module_state, options} ->
        if options[:dispatcher] && options[:dispatcher] != dispatcher do
          raise "#{inspect(module)} is setting dispatcher to #{inspect(options[:dispatcher])}, " <>
                  "which is different from dispatcher #{inspect(dispatcher)} expected by Broadway"
        end

        {:producer, %{state | module_state: module_state}, [dispatcher: dispatcher] ++ options}

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
  def handle_call({__MODULE__, :push_messages, messages}, _from, state) do
    {:reply, :ok, messages, state}
  end

  def handle_call(message, from, state) do
    %{module: module, module_state: module_state} = state

    message
    |> module.handle_call(from, module_state)
    |> case do
      {:reply, reply, events, new_module_state} ->
        messages = transform_events(events, state.transformer)
        {:reply, reply, messages, %{state | module_state: new_module_state}}

      {:reply, reply, events, new_module_state, :hibernate} ->
        messages = transform_events(events, state.transformer)
        {:reply, reply, messages, %{state | module_state: new_module_state}, :hibernate}

      {:stop, reason, reply, new_module_state} ->
        {:stop, reason, reply, %{state | module_state: new_module_state}}

      other ->
        handle_no_reply(other, state)
    end
  end

  @impl true
  def handle_cast({__MODULE__, :prepare_for_draining}, state) do
    %{module: module, module_state: module_state} = state

    if function_exported?(module, :prepare_for_draining, 1) do
      module.prepare_for_draining(module_state)
    end

    {:noreply, [], state}
  end

  def handle_cast(message, state) do
    %{module: module, module_state: module_state} = state

    message
    |> module.handle_cast(module_state)
    |> handle_no_reply(state)
  end

  @impl true
  def handle_info({__MODULE__, :cancel_consumers}, state) do
    for from <- state.consumers do
      send(self(), {:"$gen_producer", from, {:cancel, :shutdown}})
    end

    {:noreply, [], state}
  end

  def handle_info(message, state) do
    %{module: module, module_state: module_state} = state

    message
    |> module.handle_info(module_state)
    |> handle_no_reply(state)
  end

  @impl true
  def terminate(reason, %{module: module, module_state: module_state}) do
    if function_exported?(module, :terminate, 2) do
      module.terminate(reason, module_state)
    else
      :ok
    end
  end

  defp handle_no_reply(reply, %{transformer: transformer} = state) do
    case reply do
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
