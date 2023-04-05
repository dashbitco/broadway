defmodule Broadway.NoopAcknowledger do
  @moduledoc """
  An acknowledger that does nothing.

  If you want to use this acknowledger in messages produced by your
  `Broadway.Producer`, you can get its configuration by calling
  the `init/0` function. For example, you can use it in
  `Broadway.test_message/3`:

      Broadway.test_message(MyPipeline, "some data", acknowledger: Broadway.NoopAcknowledger.init())

  Broadway sets this acknowledger automatically on messages that have been acked
  via `Broadway.Message.ack_immediately/1`.
  """

  @behaviour Broadway.Acknowledger

  @doc """
  Returns the acknowledger metadata.
  """
  @spec init() :: Broadway.Message.acknowledger()
  def init do
    {__MODULE__, _ack_ref = nil, _data = nil}
  end

  @impl true
  def ack(_ack_ref = nil, _successful, _failed) do
    :ok
  end
end
