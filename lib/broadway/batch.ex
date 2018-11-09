defmodule Broadway.Batch do
  defstruct [
    :events,
    :partition,
    :batcher
  ]
end
