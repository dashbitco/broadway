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
          data: any,
          acknowledger: {module, data :: any},
          batcher: atom,
          partition: term,
          status: :ok | {:failed, reason :: binary}
        }

  @enforce_keys [:data, :acknowledger]
  defstruct data: nil,
            acknowledger: nil,
            batcher: :default,
            partition: :default,
            status: :ok

  @doc """
  Updates the data from a message.

  This function is usually used inside the `handle_message/3` implementation
  in order to replace the data with the new processed data.
  """
  @spec update_data(message :: Message.t(), fun :: (any -> any)) :: Message.t()
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
  Defines the partition within a batcher for the message.
  """
  @spec put_partition(message :: Message.t(), partition :: term) :: Message.t()
  def put_partition(%Message{} = message, partition) do
    %Message{message | partition: partition}
  end

  @doc """
  Mark a message as failed.

  Failed messages are sent directly to the related acknowledger so they're not
  forwarded to the next step in the pipeline.
  """
  @spec failed(message :: Message.t(), reason :: any) :: Message.t()
  def failed(%Message{} = message, reason) do
    %Message{message | status: {:failed, reason}}
  end
end
