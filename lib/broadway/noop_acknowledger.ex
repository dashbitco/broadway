defmodule Broadway.NoopAcknowledger do
  @moduledoc false

  @behaviour Broadway.Acknowledger

  @impl true
  def ack(_ack_ref = nil, _successful, _failed) do
    :ok
  end
end
