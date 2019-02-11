defmodule Broadway.BatchInfo do
  @moduledoc """
  A struct used to hold information about a generated batch.

  An instance of this struct containing the related info will
  be passed to the `c:Broadway.handle_batch/4` callback of the
  module implementing the `Broadway` behaviour.
  """

  @type t :: %__MODULE__{
          batcher_key: atom,
          batcher_pid: pid
        }

  defstruct [
    :batcher_key,
    :batcher_pid
  ]
end
