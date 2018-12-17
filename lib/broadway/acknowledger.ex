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
end
