defmodule Broadway.BatchInfo do
  @moduledoc """
  A struct used to hold information about a generated batch.

  An instance of this struct containing the related info will
  be passed to the `c:Broadway.handle_batch/4` callback of the
  module implementing the `Broadway` behaviour.

  See the documentation for [`%Broadway.BatchInfo{}`](`__struct__/0`)
  for information on the fields.
  """

  @typedoc """
  The type for a batch info struct.
  """
  @type t :: %__MODULE__{
          batcher: atom,
          batch_key: term,
          partition: non_neg_integer | nil,
          size: pos_integer,
          trigger: atom
        }

  @doc """
  The batch info struct.

  The fields are:

    * `:batcher` - is the key that defined the batcher. This value can
      be set in the `c:Broadway.handle_message/3` callback using
      `Broadway.Message.put_batcher/2`.

    * `:batch_key` - identifies the batch key for this batch.
      See `Broadway.Message.put_batch_key/2`.

    * `:partition` - the partition, if present.

    * `:size` - the number of messages in the batch.

    * `:trigger` - the trigger that generated the batch, like `:timeout`
      or `:flush`.

  """
  defstruct [
    :batcher,
    :batch_key,
    :partition,
    :size,
    :trigger
  ]
end
