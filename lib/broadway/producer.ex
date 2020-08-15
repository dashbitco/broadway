defmodule Broadway.Producer do
  @moduledoc """
  A Broadway.Producer is a `GenStage` producer that emits
  `Broadway.Message` as events.

  Optionally, a `Broadway.Producer` can implement two
  optional Broadway callbacks: `c:prepare_for_start/2`
  and `c:prepare_for_draining/2`, which are useful for
  booting up and shutting down Broadway topologies
  respectively.
  """

  @doc """
  Invoked once by Broadway during `Broadway.start_link/2`.

  The goal of this task is to manipulate the general topology options,
  if necessary at all, and introduce any new child specs that will be
  started before the ProducerSupervisor in Broadwday's supervision tree.

  It is guaranteed to be invoked inside the Broadway main process.

  The options include all of Broadway topology options.
  """
  @callback prepare_for_start(module :: atom, options :: keyword) ::
              {[:supervisor.child_spec() | {module, any} | module], options :: keyword}

  @doc """
  Invoked by the terminator right before Broadway starts draining in-flight
  messages during shutdown.

  This callback should be implemented by producers that need to do additional
  work before shutting down. That includes active producers like RabbitMQ that
  must ask the data provider to stop sending messages. It will be invoked for
  each producer stage.
  """
  @callback prepare_for_draining(state :: any) ::
              {:noreply, [event], new_state}
              | {:noreply, [event], new_state, :hibernate}
              | {:stop, reason :: term, new_state}
            when new_state: term, event: term

  @optional_callbacks prepare_for_start: 2, prepare_for_draining: 1
end
