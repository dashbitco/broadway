defmodule Broadway.Producer do
  @moduledoc false
  use GenStage

  alias Broadway.{Message, RateLimiter}

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
  @callback prepare_for_draining(state :: any) ::
              {:noreply, [event], new_state}
              | {:noreply, [event], new_state, :hibernate}
              | {:stop, reason :: term, new_state}
            when new_state: term, event: term

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
    # First we set the demand to accumulate. This is to avoid
    # polling implementations from re-entering the polling loop
    # once they flush any timers during draining. Push implementations
    # will still empty out their queues as long as they put them
    # in the GenStage buffer.
    GenStage.demand(producer, :accumulate)
    GenStage.cast(producer, {__MODULE__, :prepare_for_draining})
    GenStage.async_info(producer, {__MODULE__, :cancel_consumers})
  end

  @impl true
  def init({args, index}) do
    {module, arg} = args[:module]
    transformer = args[:transformer]
    dispatcher = args[:dispatcher]
    broadway_name = args[:broadway][:name]
    rate_limiting_options = args[:rate_limiting]

    # Inject the topology index only if the args are a keyword list.
    arg =
      if Keyword.keyword?(arg) do
        Keyword.put(arg, :broadway, Keyword.put(args[:broadway], :index, index))
      else
        arg
      end

    rate_limiting_state =
      if rate_limiting_options do
        %{
          state: :open,
          table_name: RateLimiter.table_name(broadway_name),
          # A queue of "batches" of messages that we buffered.
          message_buffer: :queue.new(),
          demand_buffer: []
        }
      else
        nil
      end

    state = %{
      module: module,
      module_state: nil,
      transformer: transformer,
      consumers: [],
      rate_limiting: rate_limiting_state
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
        {state, messages} = maybe_rate_limit_and_buffer_messages(state, messages)
        {:noreply, messages, %{state | module_state: new_module_state}}

      {:noreply, events, new_module_state, :hibernate} ->
        messages = transform_events(events, transformer)
        {state, messages} = maybe_rate_limit_and_buffer_messages(state, messages)
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
        {state, messages} = maybe_rate_limit_and_buffer_messages(state, messages)
        {:reply, reply, messages, %{state | module_state: new_module_state}}

      {:reply, reply, events, new_module_state, :hibernate} ->
        messages = transform_events(events, state.transformer)
        {state, messages} = maybe_rate_limit_and_buffer_messages(state, messages)
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
      module_state
      |> module.prepare_for_draining()
      |> handle_no_reply(state)
    else
      {:noreply, [], state}
    end
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

  def handle_info({RateLimiter, :reset_rate_limiting}, state) do
    state = put_in(state.rate_limiting.state, :open)

    {state, messages_to_send} = rate_limit_and_buffer_messages(state)
    {:noreply, messages_to_send, state}
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
        {state, messages} = maybe_rate_limit_and_buffer_messages(state, messages)
        {:noreply, messages, %{state | module_state: new_module_state}}

      {:noreply, events, new_module_state, :hibernate} ->
        messages = transform_events(events, transformer)
        {state, messages} = maybe_rate_limit_and_buffer_messages(state, messages)
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

  defp maybe_rate_limit_and_buffer_messages(state, messages) do
    if state.rate_limiting do
      state = update_in(state.rate_limiting.message_buffer, &enqueue_batch(&1, messages))
      rate_limit_and_buffer_messages(state)
    else
      {state, messages}
    end
  end

  defp rate_limit_and_buffer_messages(%{rate_limiting: %{state: :closed}} = state) do
    {state, []}
  end

  defp rate_limit_and_buffer_messages(%{rate_limiting: %{message_buffer: batches_buffer}} = state) do
    allowed = RateLimiter.get_currently_allowed(state.rate_limiting.table_name)

    {allowed_left, probably_sendable, batches_buffer} = dequeue_many(batches_buffer, allowed, [])

    {sendable, unsent} = rate_limit_messages(state, probably_sendable, allowed - allowed_left)

    new_buffer = :queue.in_r(unsent, batches_buffer)
    state = put_in(state.rate_limiting.message_buffer, new_buffer)

    state =
      if allowed_left == 0 or unsent == [] do
        state
      else
        put_in(state.rate_limiting.state, :closed)
      end

    {state, sendable}
  end

  defp reverse_split_demand(rest, 0, acc) do
    {0, acc, rest}
  end

  defp reverse_split_demand([], demand, acc) do
    {demand, acc, []}
  end

  defp reverse_split_demand([head | tail], demand, acc) do
    reverse_split_demand(tail, demand - 1, [head | acc])
  end

  defp dequeue_many(queue, demand, acc) do
    case :queue.out(queue) do
      {{:value, list}, queue} ->
        case reverse_split_demand(list, demand, acc) do
          {0, acc, []} ->
            {0, Enum.reverse(acc), queue}

          {0, acc, rest} ->
            {0, Enum.reverse(acc), :queue.in_r(rest, queue)}

          {demand, acc, []} ->
            dequeue_many(queue, demand, acc)
        end

      {:empty, queue} ->
        {demand, Enum.reverse(acc), queue}
    end
  end

  defp enqueue_batch(queue, _list = []), do: queue
  defp enqueue_batch(queue, list), do: :queue.in(list, queue)

  defp rate_limit_messages(_state, [], _count) do
    {[], []}
  end

  defp rate_limit_messages(state, messages, message_count) do
    case RateLimiter.rate_limit(state.rate_limiting.table_name, message_count) do
      :ok -> {messages, _unsent = []}
      {:rate_limited, overflow} -> Enum.split(messages, overflow)
    end
  end
end
