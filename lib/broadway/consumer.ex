defmodule Broadway.Consumer do
  @moduledoc false
  use GenStage
  use Broadway.Subscriber

  alias Broadway.{Acknowledger, Message}
  @subscription_options [max_demand: 1, min_demand: 0]

  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(args) do
    state = %{
      module: args[:module],
      context: args[:context]
    }

    Broadway.Subscriber.init(
      :consumer,
      :never,
      [args[:batcher]],
      @subscription_options,
      state
    )
  end

  @impl true
  def handle_events(events, _from, state) do
    [{messages, batch_info}] = events
    %Broadway.BatchInfo{publisher_key: publisher_key} = batch_info

    {successful_messages, failed_messages} =
      handle_batch(publisher_key, messages, batch_info, state)
      |> Enum.split_with(&(&1.status == :ok))

    Acknowledger.ack_messages(successful_messages, failed_messages)

    {:noreply, [], state}
  end

  defp handle_batch(publisher_key, messages, batch_info, state) do
    %{module: module, context: context} = state

    try do
      module.handle_batch(publisher_key, messages, batch_info, context)
    rescue
      e ->
        error_message = Exception.message(e)
        Enum.map(messages, &Message.failed(&1, error_message))
    end
  end
end
