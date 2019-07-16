defmodule Broadway.DummyProducer do
  @moduledoc """
  A producer that does nothing, used mostly for testing.

  See "Testing" section in `Broadway` module documentation for more information.
  """

  use GenStage
  @behaviour Broadway.Producer

  @impl true
  def init(_args) do
    {:producer, []}
  end

  @impl true
  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end
end
