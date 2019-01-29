defmodule Broadway.Acknowledger do
  @moduledoc """
  A behaviour used to acknowledge that the received messages
  were successfully processed or failed.

  When implementing a new connector for Broadway, you should
  implement this behaviour and consider how the technology
  you're working with handles message acknowledgement.

  The `c:ack/2` callback must be implemented in order to notify
  the origin of the data that a message can be safely removed
  after been successfully processed and published. In case of
  failed messages or messages without acknowledgement, depending
  on the technology chosen, the messages can be either moved back
  in the queue or, alternatively, moved to a _dead-letter queue_.
  """

  alias Broadway.Message

  @doc """
  Invoked to acknowledge successful and failed messages.

    * `successful` is the list of messages that were
      successfully processed and published.

    * `failed` is the list of messages that, for some reason,
      could not be processed or published.

  """
  @callback ack(successful :: [Message.t()], failed :: [Message.t()]) :: no_return

  @doc """
  Acknowledges successful and failed messages grouped by acknowledger.
  """
  @spec ack_messages([Message.t()], [Message.t()]) :: no_return
  def ack_messages(successful, failed) do
    %{}
    |> group_by_acknowledger(successful, :successful)
    |> group_by_acknowledger(failed, :failed)
    |> Enum.each(&call_ack/1)
  end

  defp group_by_acknowledger(grouped_messages, messages, key) do
    Enum.reduce(messages, grouped_messages, fn %{acknowledger: {acknowledger, _}} = msg, acc ->
      Map.update(acc, acknowledger, [{key, msg}], &[{key, msg} | &1])
    end)
  end

  defp call_ack({acknowledger, messages}) do
    {successful, failed} = unpack_messages(messages, [], [])
    acknowledger.ack(successful, failed)
  end

  defp unpack_messages([{:successful, message} | messages], successful, failed),
    do: unpack_messages(messages, [message | successful], failed)

  defp unpack_messages([{:failed, message} | messages], successful, failed),
    do: unpack_messages(messages, successful, [message | failed])

  defp unpack_messages([], successful, failed),
    do: {successful, failed}
end
