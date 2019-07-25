defmodule Broadway.Message do
  @moduledoc """
  A struct that holds all information about a message.

  A message is first created by the producers. Once created,
  the message is sent downstream and gets updated multiple
  times, either by the module implementing the `Broadway`
  behaviour through the `c:Broadway.handle_message/3` callback
  or internaly by one of the built-in stages of Broadway.

  In order to manipulate a message, you should use one of
  the imported functions provided by this module.
  """

  alias __MODULE__, as: Message

  @type t :: %Message{
          data: term,
          metadata: %{optional(:atom) => term},
          acknowledger: {module, ack_ref :: term, data :: term},
          batcher: atom,
          batch_key: term,
          batch_mode: :bulk | :flush,
          status: :ok | {:failed, reason :: binary}
        }

  @enforce_keys [:data, :acknowledger]
  defstruct data: nil,
            metadata: %{},
            acknowledger: nil,
            batcher: :default,
            batch_key: :default,
            batch_mode: :bulk,
            status: :ok

  @doc """
  Updates the data from a message.

  This function is usually used inside the `handle_message/3` implementation
  in order to replace the data with the new processed data.
  """
  @spec update_data(message :: Message.t(), fun :: (term -> term)) :: Message.t()
  def update_data(%Message{} = message, fun) when is_function(fun, 1) do
    %Message{message | data: fun.(message.data)}
  end

  @doc """
  Defines the target batcher which the message should be forwarded to.
  """
  @spec put_batcher(message :: Message.t(), batcher :: atom) :: Message.t()
  def put_batcher(%Message{} = message, batcher) when is_atom(batcher) do
    %Message{message | batcher: batcher}
  end

  @doc """
  Defines the batch key within a batcher for the message.

  Inside each batcher, we attempt to build `batch_size`
  within `batch_timeout` for each `batch_key`.
  """
  @spec put_batch_key(message :: Message.t(), batch_key :: term) :: Message.t()
  def put_batch_key(%Message{} = message, batch_key) do
    %Message{message | batch_key: batch_key}
  end

  @doc """
  Sets the batching mode for the message.

  When the mode is `:bulk`, the batch that the message is in is delivered after
  the batch size or the batch timeout is reached.

  When the mode is `:flush`, the batch that the message is in is delivered
  immediately.

  The default mode for messages is `:bulk`.
  """
  @spec put_batch_mode(message :: Message.t(), mode :: :bulk | :flush) :: Message.t()
  def put_batch_mode(%Message{} = message, mode) when mode in [:bulk, :flush] do
    %Message{message | batch_mode: mode}
  end

  @doc """
  Mark a message as failed.

  Failed messages are sent directly to the related acknowledger so they're not
  forwarded to the next step in the pipeline.
  """
  @spec failed(message :: Message.t(), reason :: term) :: Message.t()
  def failed(%Message{} = message, reason) do
    %Message{message | status: {:failed, reason}}
  end
end
