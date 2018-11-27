defmodule Broadway.Producer do
  use GenStage

  defmodule State do
    defstruct [:module, :state]
  end

  def start_link(args, opts \\ []) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  def push_message(producer, message) do
    GenStage.call(producer, {:push_message, message})
  end

  def init(args) do
    module = Keyword.fetch!(args, :module)
    module_args = Keyword.fetch!(args, :args)
    {:producer, state} = module.init(module_args)
    {:producer, %State{module: module, state: state}}
  end

  def handle_demand(demand, %State{module: module, state: module_state} = state) do
    case module.handle_demand(demand, module_state) do
      {tag, events_or_reason, new_state} ->
        {tag, events_or_reason, %State{state | state: new_state}}

      {:noreply, events, new_state, :hibernate} ->
        {:noreply, events, %State{state | state: new_state}, :hibernate}
    end
  end

  def handle_call({:push_message, message}, _from, state) do
    {:reply, :ok, [message], state}
  end
end
