defmodule Broadway.Processor do
  @moduledoc false
  use GenStage

  alias Broadway.Message

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
    publishers_config = args[:publishers_config]
    processors_config = args[:processors_config]
    context = args[:context]
    keys = Keyword.keys(publishers_config)
    state = %State{module: args[:module], context: context}

    subscribe_options =
      Keyword.take(processors_config, [:min_demand, :max_demand]) ++ [cancel: :temporary]

    subscribe_to =
      args[:producers]
      |> Enum.map(&{&1, subscribe_options})

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
