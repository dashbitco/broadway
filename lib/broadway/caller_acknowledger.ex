defmodule Broadway.CallerAcknowledger do
  @moduledoc """
  A simple acknowledger that sends a message back to a caller.

  If you want to use this acknowledger in messages produced by your
  `Broadway.Producer`, you can get its configuration by calling
  the `init/0` function. For example, you can use it in
  `Broadway.test_message/3`:

      some_ref = make_ref()

      Broadway.test_message(
        MyPipeline,
        "some data",
        acknowledger: Broadway.CallerAcknowledger.init({self(), some_ref}, :ignored)
      )

  The first parameter is a tuple with the PID to receive the messages
  and a unique identifier (usually a reference). Such unique identifier
  is then included in the messages sent to the PID. The second parameter,
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

  See the module documentation.
  """
  @spec init({pid, ref :: term}, ignored_term :: term) :: Broadway.Message.acknowledger()
  def init({pid, ref} = _pid_and_ref, ignored_term) when is_pid(pid) do
    {__MODULE__, {pid, ref}, ignored_term}
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
