defmodule Broadway.NoopAcknowledger do
  @moduledoc """
  An acknowledger that does nothing.

  Set automatically on messages that have been acked immediately
  via `Broadway.Message.ack_immediately/1`.
  """

  @behaviour Broadway.Acknowledger

  @impl true
  def ack(nil = _ack_ref, _successful, _failed) do
    :ok
  end
end
