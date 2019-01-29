defmodule Broadway.Message do
  @moduledoc """
  A struct that holds all information about a message.

  A message is first created by the producers. Once created,
  the message is sent downstream and gets updated multiple
  times, either by the module implementing the `Broadway`
  behaviour through the `c:Broadway.handle_message/2` callback
  or internaly by one of the built-in stages of Broadway.

  In order to manipulate a message, you should use one of
  the imported functions provided by this module.
  """

  alias __MODULE__, as: Message

  @type t :: %Message{
          data: any,
          acknowledger: {module, data :: any},
          publisher: atom,
          processor_pid: pid,
          status: :pending | :ok | {:failed, reason :: binary}
        }

  defstruct data: nil,
            acknowledger: nil,
            publisher: :default,
            processor_pid: nil,
            status: :pending

  @doc """
  Updates the data from a message.

  This function is usually used inside the `handle_message/2` implementation
  in order to replace the data with the new processed data.
  """
  @spec update_data(message :: Message.t(), fun :: (any -> any)) :: Message.t()
  def update_data(%Message{} = message, fun) when is_function(fun, 1) do
    %Message{message | data: fun.(message.data)}
  end

  @doc """
  Defines the target publisher which the message should be forwarded to.
  """
  @spec put_publisher(message :: Message.t(), publisher :: atom) :: Message.t()
  def put_publisher(%Message{} = message, publisher) when is_atom(publisher) do
    %Message{message | publisher: publisher}
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
