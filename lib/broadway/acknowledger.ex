defmodule Broadway.Acknowledger do
  @moduledoc """
  A behaviour used to acknowledge that the received messages
  were successfully processed or failed.

  When implementing a new connector for Broadway, you should
  implement this behaviour and consider how the technology
  you're working with handles message acknowledgement.

  The `c:ack/3` callback must be implemented in order to notify
  the origin of the data that a message can be safely removed
  after been successfully processed and published. In case of
  failed messages or messages without acknowledgement, depending
  on the technology chosen, the messages can be either moved back
  in the queue or, alternatively, moved to a *dead-letter queue*.
  """

  alias Broadway.Message

  require Logger

  @doc """
  Invoked to acknowledge successful and failed messages.

    * `ack_ref` is a term that uniquely identifies how messages
      should be grouped and sent for acknowledgement. Imagine
      you have a scenario where messages are coming from
      different producers. Broadway will use this information
      to correctly identify the acknowledger and pass it among
      with the messages so you can properly communicate with
      the source of the data for acknowledgement. `ack_ref` is
      part of `t:Broadway.Message.acknowledger/0`.

    * `successful` is the list of messages that were
      successfully processed and published.

    * `failed` is the list of messages that, for some reason,
      could not be processed or published.

  """
  @callback ack(ack_ref :: term, successful :: [Message.t()], failed :: [Message.t()]) ::
              :ok

  @doc """
  Configures the acknowledger with new `options`.

  Every acknowledger can decide how to incorporate the given `options` into its
  `ack_data`. The `ack_data` is the current acknowledger's data. The return value
  of this function is `{:ok, new_ack_data}` where `new_ack_data` is the updated
  data for the acknowledger.

  Note that `options` are different for every acknowledger, as the acknowledger
  is what specifies what are the supported options. Check the documentation for the
  acknowledger you're using to see the supported options.

  `ack_ref` and `ack_data` are part of `t:Broadway.Message.acknowledger/0`.
  """
  @callback configure(ack_ref :: term, ack_data :: term, options :: keyword) ::
              {:ok, new_ack_data :: term}

  @optional_callbacks [configure: 3]

  @doc false
  @spec ack_messages([Message.t()], [Message.t()]) :: no_return
  def ack_messages(successful, failed) do
    %{}
    |> group_by_acknowledger(successful, :successful)
    |> group_by_acknowledger(failed, :failed)
    |> Enum.each(&call_ack/1)
  end

  defp group_by_acknowledger(ackers, messages, key) do
    Enum.reduce(messages, ackers, fn %{acknowledger: {acknowledger, ack_ref, _}} = msg, acc ->
      ack_info = {acknowledger, ack_ref}
      pdict_key = {ack_info, key}
      Process.put(pdict_key, [msg | Process.get(pdict_key, [])])
      Map.put(acc, ack_info, true)
    end)
  end

  defp call_ack({{acknowledger, ack_ref} = ack_info, true}) do
    successful = Process.delete({ack_info, :successful}) || []
    failed = Process.delete({ack_info, :failed}) || []
    acknowledger.ack(ack_ref, Enum.reverse(successful), Enum.reverse(failed))
  end

  @doc false
  # Builds a crash reason used in Logger reporting.
  def crash_reason(:throw, reason, stack), do: {{:nocatch, reason}, stack}
  def crash_reason(:error, reason, stack), do: {Exception.normalize(:error, reason, stack), stack}
  def crash_reason(:exit, reason, stack), do: {reason, stack}

  # Used by the processor and the batcher to maybe call c:handle_failed/2
  # on failed messages.
  @doc false
  def maybe_handle_failed_messages(messages, module, context) do
    if function_exported?(module, :handle_failed, 2) and messages != [] do
      handle_failed_messages(messages, module, context)
    else
      messages
    end
  end

  defp handle_failed_messages(messages, module, context) do
    module.handle_failed(messages, context)
  catch
    kind, reason ->
      Logger.error(Exception.format(kind, reason, __STACKTRACE__),
        crash_reason: crash_reason(kind, reason, __STACKTRACE__)
      )

      messages
  else
    return_messages when is_list(return_messages) ->
      size = length(messages)
      return_size = length(return_messages)

      if return_size != size do
        Logger.error(
          "#{inspect(module)}.handle_failed/2 received #{size} messages and " <>
            "returned only #{return_size}. All messages given to handle_failed/2 " <>
            "must be returned"
        )
      end

      return_messages

    _other ->
      Logger.error(
        "#{inspect(module)}.handle_failed/2 didn't return a list of messages, " <>
          "so ignoring its return value"
      )

      messages
  end
end
