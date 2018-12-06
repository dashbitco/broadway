defmodule Broadway.Processor do
  @moduledoc false
  use GenStage

  alias Broadway.Message

  @default_min_demand 2
  @default_max_demand 4

  defmodule State do
    @moduledoc false
    defstruct [:module, :context]
  end

  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  def child_spec(args) do
    %{start: {__MODULE__, :start_link, args}}
  end

  def init(args) do
    publishers_config = Keyword.fetch!(args, :publishers_config)
    processors_config = Keyword.fetch!(args, :processors_config)
    context = Keyword.fetch!(args, :context)
    keys = Keyword.keys(publishers_config)
    min_demand = Keyword.get(processors_config, :min_demand, @default_min_demand)
    max_demand = Keyword.get(processors_config, :max_demand, @default_max_demand)
    state = %State{module: Keyword.fetch!(args, :module), context: context}

    subscribe_to =
      args
      |> Keyword.fetch!(:producers)
      |> Enum.map(&{&1, max_demand: max_demand, min_demand: min_demand, cancel: :temporary})

    {:producer_consumer, state,
     subscribe_to: subscribe_to,
     dispatcher: {GenStage.PartitionDispatcher, partitions: keys, hash: & &1}}
  end

  def handle_events(messages, _from, state) do
    %State{module: module, context: context} = state

    events =
      Enum.map(messages, fn message ->
        new_message = %Message{message | processor_pid: self()}
        {:ok, new_message} = module.handle_message(new_message, context)
        %Message{publisher: publisher} = new_message
        {new_message, publisher}
      end)

    {:noreply, events, state}
  end
end
