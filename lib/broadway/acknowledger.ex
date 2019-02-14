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
  in the queue or, alternatively, moved to a *dead-letter queue*.
  """

  alias Broadway.Message

  @doc """
  Invoked to acknowledge successful and failed messages.

    * `ack_ref` is a term that uniquely identifies how messages
      should be grouped and sent for acknowledgement. Imagine
      you have a scenario where messages are coming from
      different producers. Broadway will use this information
      to correctly identify the acknowledger and pass it among
      with the messages so you can property communicate with
      the source of the data for acknowledgement.

    * `successful` is the list of messages that were
      successfully processed and published.

    * `failed` is the list of messages that, for some reason,
      could not be processed or published.

  """
  @callback ack(ack_ref :: reference, successful :: [Message.t()], failed :: [Message.t()]) ::
              no_return

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

  defp group_by_acknowledger(ackers, messages, key) do
    Enum.reduce(messages, ackers, fn %{acknowledger: {acknowledger, ack_ref, _}} = msg, acc ->
      ack_info = {acknowledger, ack_ref}
      pdict_key = {ack_info, key}
      Process.put(pdict_key, [msg | Process.get({ack_info, key}, [])])
      Map.put(acc, ack_info, true)
    end)
  end

  defp call_ack({{acknowledger, ack_ref} = ack_info, true}) do
    successful = Process.delete({ack_info, :successful}) || []
    failed = Process.delete({ack_info, :failed}) || []
    acknowledger.ack(ack_ref, Enum.reverse(successful), Enum.reverse(failed))
  end
end
