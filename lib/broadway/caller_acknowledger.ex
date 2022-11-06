defmodule Broadway.CallerAcknowledger do
  @moduledoc """
  A simple acknowledger that sends a message back to a caller.

  It must be stored as:

      acknowledger: Broadway.CallerAcknowledger.init({pid, ref}, term)

  The first parameter is a tuple with the pid to receive the messages
  and a unique identifier (usually a reference). The second parameter,
  which is per message, is ignored.

  It sends a message in the format:

      {:ack, ref, successful_messages, failed_messages}

  If `Broadway.Message.configure_ack/2` is called on a message that
  uses this acknowledger, then the following message is sent:

      {:configure, ref, options}

  """

  @behaviour Broadway.Acknowledger

  @doc """
  Returns the acknowledger metadata.
  """
  def init({pid, ref}, term) do
    {__MODULE__, {pid, ref}, term}
  end

  @impl true
  def ack({pid, ref}, successful, failed) do
    send(pid, {:ack, ref, successful, failed})
  end

  @impl true
  def configure({pid, ref}, ack_data, options) do
    send(pid, {:configure, ref, options})
    {:ok, ack_data}
  end
end
