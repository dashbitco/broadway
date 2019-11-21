defmodule Broadway.LineProducer do
  @moduledoc """
  A GenStage producer that receives one message at a time
  from an upstream pipeline.

  In theatre, a Line Producer manages the budget of a
  production, and generally only works on one production
  at a time. `Broadway.LineProducer` plays a similar role,
  as it budgets the time to dispatch messages redirected
  to its pipeline, and it only accepts one new message at a
  time.

  This implementation will keep messages in an internal
  queue until there is demand, leading to client timeouts
  for slow consumers.

  You can use the mental picture of a line of people
  (messages) waiting for their turn to be dispatched.
  Messages enter at the rear (tail) of the line and are
  dispatched from the front (head) of the line.

  See the "Chaining Pipelines" section in the `Broadway`
  module documentation for more information.
  """
  use GenStage
  alias Broadway.Producer

  @behaviour Producer

  def start_link(opts \\ []) do
    {server_opts, opts} = Keyword.split(opts, [:name])
    GenStage.start_link(__MODULE__, opts, server_opts)
  end

  @doc """
  Sends a `Broadway.Message` and returns only after the
  message is dispatched.
  """
  @spec dispatch(GenServer.server(), Message.t(), timeout :: GenServer.timeout()) :: :ok
  def dispatch(line_producer, message, timeout \\ 5000) do
    GenStage.call(line_producer, {__MODULE__, :dispatch, message}, timeout)
  end

  ## Callbacks

  @impl true
  def init(_opts) do
    {:producer, {:queue.new(), 0}}
  end

  @impl true
  def handle_call({__MODULE__, :dispatch, message}, from, {queue, demand}) do
    dispatch_messages(:queue.in({from, message}, queue), demand, [])
  end

  @impl true
  def handle_demand(incoming_demand, {queue, demand}) do
    dispatch_messages(queue, incoming_demand + demand, [])
  end

  defp dispatch_messages(queue, demand, messages) do
    with d when d > 0 <- demand,
         {{:value, {from, message}}, queue} <- :queue.out(queue) do
      GenStage.reply(from, :ok)
      dispatch_messages(queue, demand - 1, [message | messages])
    else
      _ -> {:noreply, Enum.reverse(messages), {queue, demand}}
    end
  end
end
