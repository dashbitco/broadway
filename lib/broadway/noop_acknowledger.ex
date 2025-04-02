defmodule Broadway.NoopAcknowledger do
  @moduledoc """
  An acknowledger that performs no operations.

  Use this module to configure messages produced by `Broadway.Producer`
  when no acknowledgment is needed.

  ## Example

  Use with `Broadway.test_message/3` for testing purposes:

      Broadway.test_message(
        MyPipeline,
        "some data",
        acknowledger: Broadway.NoopAcknowledger.init()
      )

  This acknowledger is automatically set by Broadway for messages
  acknowledged via `Broadway.Message.ack_immediately/1`.
  """

  @behaviour Broadway.Acknowledger

  @doc """
  Returns a no-op acknowledger tuple.

  The tuple format `{module, ack_ref, data}` satisfies the
  `Broadway.Acknowledger` contract without performing any operation.

  ## Example

      iex> Broadway.NoopAcknowledger.init()
      {Broadway.NoopAcknowledger, nil, nil}
  """
  @spec init() :: Broadway.Message.acknowledger()
  def init, do: {__MODULE__, nil, nil}

  @impl true
  @doc false
  def ack(_ack_ref, _successful, _failed), do: :ok
end
