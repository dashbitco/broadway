defmodule Broadway.Message do
  @moduledoc """
  This struct holds all information about a message.

  A message is first created by the producers. It is then
  sent downstream and gets updated multiple times, either
  by a module implementing the `Broadway` behaviour
  through the `c:Broadway.handle_message/3` callback
  or internally by one of the built-in stages of Broadway.

  Instead of modifying the struct directly, you should use the functions
  provided by this module to manipulate messages. However, if you are implementing
  a `Broadway.Producer` of your own, see `t:t/0` to see what fields you should set.
  """

  alias __MODULE__, as: Message
  alias Broadway.{Acknowledger, NoopAcknowledger}

  @typedoc """
  The acknowledger of the message.

  This tuple contains:

    * A module implementing the `Broadway.Acknowledger` behaviour.

    * An ack reference that is passed to the `c:Broadway.Acknowledger.ack/3`
      callback. See `c:Broadway.Acknowledger.ack/3` for more information.

    * An arbitrary term that is passed to the optional
      `c:Broadway.Acknowledger.configure/3` callback.

  """
  @typedoc since: "1.1.0"
  @type acknowledger :: {module, ack_ref :: term, data :: term}

  @typedoc """
  The Broadway message struct.

  Most of these fields are manipulated by Broadway itself. You can
  *read* the `:metadata` field, and you can use the functions in this
  module to update most of the other fields. If you are implementing
  your own producer, see the `Broadway.Producer` documentation
  for more information on how to create and manipulate message structs.
  """
  @type t :: %Message{
          data: term,
          metadata: %{optional(atom) => term},
          acknowledger: acknowledger,
          batcher: atom,
          batch_key: term,
          batch_mode: :bulk | :flush,
          status:
            :ok
            | {:failed, reason :: term}
            | {:throw | :error | :exit, term, Exception.stacktrace()}
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
  Updates the data in the message.

  This function is usually used inside the `c:Broadway.handle_message/3` implementation
  to update data with new processed data.
  """
  @spec update_data(message :: Message.t(), fun :: (term -> term)) :: Message.t()
  def update_data(%Message{} = message, fun) when is_function(fun, 1) do
    %{message | data: fun.(message.data)}
  end

  @doc """
  Stores the given data in the message.

  This function is usually used inside the `c:Broadway.handle_message/3` implementation
  to replace data with new processed data.
  """
  @doc since: "1.0.0"
  @spec put_data(message :: Message.t(), term) :: Message.t()
  def put_data(%Message{} = message, data) do
    %{message | data: data}
  end

  @doc """
  Defines the target batcher which the message should be forwarded to.
  """
  @spec put_batcher(message :: Message.t(), batcher :: atom) :: Message.t()
  def put_batcher(%Message{} = message, batcher) when is_atom(batcher) do
    %{message | batcher: batcher}
  end

  @doc """
  Defines the message batch key.

  The batch key identifies the batch the message belongs to, within
  a given batcher. Each batcher then groups batches with the same
  `batch_key`, with size of at most `batch_size` within period
  `batch_timeout`. Both `batch_size` and `batch_timeout` are managed
  per batch key, so a batcher is capable of grouping multiple batch
  keys at the same time, regardless of the concurrency level.

  If a given batcher has multiple batch processors (concurrency > 1),
  all messages with the same batch key are routed to the same processor.
  So different batch keys may run concurrently but the same batch key
  is always run serially and in the same batcher processor.
  """
  @spec put_batch_key(message :: Message.t(), batch_key :: term) :: Message.t()
  def put_batch_key(%Message{} = message, batch_key) do
    %{message | batch_key: batch_key}
  end

  @doc """
  Sets the batching mode for the message.

  When the mode is `:bulk`, the batch that the message is in is delivered after
  the batch size or batch timeout is reached.

  When the mode is `:flush`, the batch that the message is in is delivered
  immediately after processing. Note it doesn't mean the batch contains only a single element
  but rather that all messages received from the processor are delivered without waiting.

  The default mode for messages is `:bulk`.
  """
  @spec put_batch_mode(message :: Message.t(), mode :: :bulk | :flush) :: Message.t()
  def put_batch_mode(%Message{} = message, mode) when mode in [:bulk, :flush] do
    %{message | batch_mode: mode}
  end

  @doc """
  Configures the acknowledger of this message.

  This function calls the `c:Broadway.Acknowledger.configure/3` callback to
  change the configuration of the acknowledger for the given `message`.

  This function can only be called if the acknowledger implements the `configure/3`
  callback. If it doesn't, an error is raised.
  """
  @doc since: "0.5.0"
  @spec configure_ack(message :: Message.t(), options :: keyword) :: Message.t()
  def configure_ack(%Message{} = message, options) when is_list(options) do
    %{acknowledger: {module, ack_ref, ack_data}} = message

    if Code.ensure_loaded?(module) and function_exported?(module, :configure, 3) do
      {:ok, ack_data} = module.configure(ack_ref, ack_data, options)
      %{message | acknowledger: {module, ack_ref, ack_data}}
    else
      raise "the configure/3 callback is not defined by acknowledger #{inspect(module)}"
    end
  end

  @doc """
  Mark a message as failed.

  Failed messages are sent directly to the related acknowledger at the end
  of this step and therefore they're not forwarded to the next step in the
  pipeline.

  Failing a message does not emit any log but it does trigger the
  `c:Broadway.handle_failed/2` callback.
  """
  @spec failed(message :: Message.t(), reason :: term) :: Message.t()
  def failed(%Message{} = message, reason) do
    %{message | status: {:failed, reason}}
  end

  @doc """
  Immediately acknowledges the given message or list of messages.

  This function can be used to acknowledge a message (or list of messages)
  immediately without waiting for the rest of the pipeline.

  Acknowledging a message sets that message's acknowledger to a no-op
  acknowledger so that it's safe to ack at the end of the pipeline.

  Returns the updated acked message if a message is passed in,
  or the updated list of acked messages if a list of messages is passed in.
  """
  @doc since: "0.5.0"
  @spec ack_immediately(message :: Message.t()) :: Message.t()
  @spec ack_immediately(messages :: [Message.t(), ...]) :: [Message.t(), ...]
  def ack_immediately(message_or_messages)

  def ack_immediately(%Message{} = message) do
    [message] = ack_immediately([message])
    message
  end

  def ack_immediately(messages) when is_list(messages) and messages != [] do
    {successful, failed} = Enum.split_with(messages, &(&1.status == :ok))
    _ = Acknowledger.ack_messages(successful, failed)

    for message <- messages do
      %{message | acknowledger: NoopAcknowledger.init()}
    end
  end
end
