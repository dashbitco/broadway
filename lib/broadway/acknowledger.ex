defmodule Broadway.Acknowledger do
  alias Broadway.Message

  @callback ack(successful :: [Message.t()], failed :: [Message.t()]) :: no_return
end
