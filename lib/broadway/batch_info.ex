defmodule Broadway.BatchInfo do
  @type t :: %__MODULE__{
          publisher_key: atom,
          batcher: pid
        }

  defstruct [
    :publisher_key,
    :batcher
  ]
end
