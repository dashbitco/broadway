defmodule Broadway.NoopAcknowledger do
  @moduledoc """
  An acknowledger that does nothing.

  It must be initialized as:

      acknowledger: Broadway.NoopAcknowledger.init()

  Set automatically on messages that have been acked immediately
  via `Broadway.Message.ack_immediately/1`.
  """

  @behaviour Broadway.Acknowledger

  def init do
    {NoopAcknowledger, _ack_ref = nil, _data = nil}
  end

  @impl true
  def ack(_ack_ref = nil, _successful, _failed) do
    :ok
  end
end
