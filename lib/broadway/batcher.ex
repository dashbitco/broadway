defmodule Broadway.Batcher do
  use GenStage

  defmodule State do
    defstruct [
      :batch_size,
      :batch_timeout,
      :publisher_key,
      :pending_events
    ]
  end

  def start_link(args, opts) do
    GenStage.start_link(__MODULE__, args, opts)
  end

  def child_spec(args) do
    %{start: {__MODULE__, :start_link, args}}
  end

  def init(args) do
    batch_timeout = Keyword.get(args, :batch_timeout, 1000)
    batch_size = Keyword.get(args, :batch_size, 100)
    publisher_key = Keyword.fetch!(args, :publisher_key)

    subscribe_to =
      args
      |> Keyword.fetch!(:processors)
      |> Enum.map(&{&1, partition: publisher_key, max_demand: 4, min_demand: 2})

    schedule_flush_pending(batch_timeout)

    {
      :producer_consumer,
      %State{
        publisher_key: publisher_key,
        batch_size: batch_size,
        batch_timeout: batch_timeout,
        pending_events: []
      },
      subscribe_to: subscribe_to
    }
  end

  def handle_events(events, _from, state) do
    %State{pending_events: pending_events, batch_size: batch_size} = state
    do_handle_events(pending_events ++ events, state, batch_size)
  end

  def handle_info(:flush_pending, state) do
    %State{pending_events: pending_events, batch_timeout: batch_timeout} = state
    schedule_flush_pending(batch_timeout)
    do_handle_events(pending_events, state, 1)
  end

  defp do_handle_events(events, state, min_size) do
    %State{batch_size: batch_size, publisher_key: publisher_key} = state
    {batch_events, new_pending_events} = split_events(events, publisher_key, batch_size, min_size)

    {:noreply, batch_events, %State{state | pending_events: new_pending_events}}
  end

  defp split_events(events, publisher_key, batch_size, min_size) do
    {batch_events, pending_events} = Enum.split(events, batch_size)

    if length(batch_events) >= min_size do
      {[%Broadway.Batch{messages: batch_events, publisher_key: publisher_key, batcher: self()}],
       pending_events}
    else
      {[], events}
    end
  end

  defp schedule_flush_pending(delay) do
    Process.send_after(self(), :flush_pending, delay)
  end
end
