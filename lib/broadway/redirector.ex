defmodule Broadway.Redirector do
  # Redirection logic for routing messages to a proceeding pipeline.
  @moduledoc false
  alias Broadway.{LineProducer, Message, NoopAcknowledger, Server}

  @spec redirect(
          server :: GenServer.server(),
          batch :: [Message.t(), ...],
          opts :: any
        ) :: [Message.t(), ...]
  def redirect(server, messages, opts \\ []) when is_list(messages) and messages != [] do
    # skip redirecting failed and/or acknowledged messages
    {redirects, skipped} =
      Enum.split_with(messages, fn
        %Message{status: :ok, acknowledger: {acknowledger, _, _}}
        when acknowledger != NoopAcknowledger ->
          true

        _ ->
          false
      end)

    # dispatch messages to producer stages, returning results and messages
    for {result, message} <- dispatch_and_return(server, redirects, opts) do
      case result do
        {:ok, message} ->
          # set a no-op acknowledger for the return to `handle_batch/4`
          %{message | acknowledger: {NoopAcknowledger, _ack_ref = nil, _ack_data = nil}}

        {:exit, reason} ->
          # failed dispatch fails the message
          Message.failed(message, reason)
      end
    end ++ skipped
  end

  @doc false
  @spec dispatch(Message.t(), GenServer.server(), GenServer.timeout()) :: Message.t() | no_return
  def dispatch(message, server, timeout) do
    producer = Enum.random(Server.producer_names(server))

    if pid = Process.whereis(producer) do
      _ =
        LineProducer.dispatch(
          pid,
          # reset the batcher values before dispatching
          %{message | batcher: :default, batch_key: :default},
          timeout
        )

      message
    else
      Message.failed(message, {:redirect, server})
    end
  end

  defp dispatch_and_return(server, messages, opts) do
    timeout = opts[:timeout] || 5000
    ordered = opts[:ordered] || false

    messages
    |> Task.async_stream(__MODULE__, :dispatch, [server, timeout],
      max_concurrency: length(Server.producer_names(server)),
      on_timeout: :kill_task,
      ordered: ordered,
      timeout: :infinity
    )
    |> Enum.zip(messages)
  end
end
