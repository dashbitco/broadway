defmodule Broadway.CallerAcknowledger do
  @moduledoc """
  A simple acknowledger that sends a message back to a caller.

  It must be stored as:

      acknowledger: {Broadway.CallerAcknowledger, {pid, ref}, term}

  The second element is a tuple with the pid to receive the messages
  and a unique identifier (usually a reference). The third element,
  which is per message, is ignored.

  It sends a message in the format:

      {:ack, ref, successful_messages, failed_messages}
  """

  @behaviour Broadway.Acknowledger

  @impl true
  def ack({pid, ref}, successful, failed) do
    send(pid, {:ack, ref, successful, failed})
  end
end
