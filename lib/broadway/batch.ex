defmodule Broadway.Batch do

  alias Broadway.Message

  @type t :: %__MODULE__{
    messages: [Message.t],
    publisher_key: atom,
    batcher: pid
  }

  defstruct [
    :messages,
    :publisher_key,
    :batcher
  ]
end
