defmodule Broadway.Processor do
  @moduledoc false
  use GenStage

  alias Broadway.Message

  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(args) do
    processors_config = args[:processors_config]
    context = args[:context]
    partitions = args[:partitions]
    state = %{module: args[:module], context: context}

    subscribe_options =
      Keyword.take(processors_config, [:min_demand, :max_demand]) ++ [cancel: :temporary]

    subscribe_to =
      args[:producers]
      |> Enum.map(&{&1, subscribe_options})

    {:producer_consumer, state,
     subscribe_to: subscribe_to,
     dispatcher: {GenStage.PartitionDispatcher, partitions: partitions, hash: & &1}}
  end

  @impl true
  def handle_events(messages, _from, state) do
    %{module: module, context: context} = state

    # TODO: Raise a proper error message if handle_message
    # does not return {:ok, %Message{}} or if the publisher
    # in the message is not known (within the upcoming try/catch block).
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
