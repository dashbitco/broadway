defmodule Broadway.NoopAcknowledger do
  @moduledoc """
  An acknowledger that does nothing.

  Set automatically on messages that have been acked immediately
  via `Broadway.Message.ack_immediately/1`.
  """

  @behaviour Broadway.Acknowledger

  @impl true
  def ack(_ack_ref = nil, _successful, _failed) do
    :ok
  end
end
