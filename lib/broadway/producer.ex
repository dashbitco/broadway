defmodule Broadway.Producer do
  @moduledoc """
  A Broadway producer is a `GenStage` producer that emits
  `Broadway.Message` structs as events.

  The `Broadway.Producer` is declared in a Broadway topology
  via the `:module` option (see `Broadway.start_link/2`):

      producer: [
        module: {MyProducer, options}
      ]

  Once declared, `MyProducer` is expected to implement and
  behave as a `GenStage` producer. When Broadway starts,
  the `c:GenStage.init/1` callback will be invoked directly with the
  given `options`.

  ## Injected Broadway configuration

  If `options` is a keyword list, Broadway injects a `:broadway` option
  into that list. This option contains the configuration for the
  complete Broadway topology (see `Broadway.start_link/2`). For example,
  use `options[:broadway][:name]` to uniquely identify the topology.

  The `:broadway` configuration also has an `:index` key. This
  is the index of the producer in its supervision tree (starting
  from `0`). This enables patterns such as connecting even-indexed producers
  to one server while odd-indexed producers connect to another.

  If `options` is any other term besides a keyword list, it is passed directly to the `c:GenStage.init/1`
  callback without modification. All other functions behave precisely as in `GenStage`
  with the requirement that all emitted events must be `Broadway.Message` structs.
  ## Optional callbacks

  A `Broadway.Producer` can implement two optional Broadway callbacks,
  `c:prepare_for_start/2` and `c:prepare_for_draining/1`, which
  boot up and shut down Broadway topologies, respectively.

  ## Producing Broadway messages

  The pipeline modifies `Broadway.Message` structs using functions
  from the `Broadway.Message` module, except for [custom producers](https://hexdocs.pm/broadway/custom-producers.html).

  Manipulate these custom producer struct fields directly:

    * `:data` (required) - the message data.
      Pass in the `:data` field directly. (Don't use `Broadway.Message.put_data/2`.)

    * `:acknowledger` (required) - the message acknowledger.
      The acknowledger's type is `t:Broadway.Message.acknowledger/0`.

    * `:metadata` (optional) - the producer-attached message metadata.
      Optionally document information for users to use in their pipelines.

  For example, a custom producer creates this message:

      %Broadway.Message{
        data: "some data here",
        acknowledger: Broadway.NoopAcknowledger.init()
      }

  """

  @doc """
  Broadway invokes this callback once during `Broadway.start_link/2`.

  This callback manipulates the general topology options
  and introduces any new child specifications that Broadway will
  start **before** the producers' supervisor in its supervision tree.
  Broadway's supervision tree operates as a `rest_for_one` supervisor
  (see `Supervisor`), so if the children returned from this callback
  crash, the entire pipeline shuts down until the supervisor restarts them.

  Broadway invokes this callback within the main Broadway process with the
  following parameters:

    * `module` - the Broadway module passed as the first argument to `Broadway.start_link/2`.

    * `options` - a keyword list of Broadway topology options passed
      as the second argument to `Broadway.start_link/2`.

  The callback returns a tuple `{child_specs, updated_options}`. `child_specs`
  is a list of child specifications from the children that Broadway will start under its supervision tree.
  `updated_options` contains potentially-modified Broadway options
  that replace those initially passed to `Broadway.start_link/2`. This
  allows adjustments to the characteristics of the Broadway topology to accommodate
  the newly introduced children.

  ## Examples

      defmodule MyProducer do
        @behaviour Broadway.Producer

        # other callbacks...

        @impl true
        def prepare_for_start(_module, broadway_options) do
           children = [
             {DynamicSupervisor, strategy: :one_for_one, name: MyApp.DynamicSupervisor}
           ]
            updated_options = put_in(broadway_options, [:producer, :rate_limiting], [interval: 1000, allowed_messages: 10])

           {children, updated_options}
        end
      end

  """
  @doc since: "0.5.0"
  @callback prepare_for_start(module :: atom, options :: keyword) ::
              {[child_spec], updated_options :: keyword}
            when child_spec: :supervisor.child_spec() | {module, any} | module

  @doc """
  The terminator invokes this callback right before Broadway starts draining in-flight
  messages during shutdown.

  Implement this callback for producers that need to do additional
  work before shutting down. That includes active producers like RabbitMQ that
  must ask the data provider to stop sending messages. Broadway will invoke this for
  each producer stage.

    * `state` - the current state of the producer.
  """
  @callback prepare_for_draining(state :: any) ::
              {:noreply, [event], new_state}
              | {:noreply, [event], new_state, :hibernate}
              | {:stop, reason :: term, new_state}
            when new_state: term, event: term

  @optional_callbacks prepare_for_start: 2, prepare_for_draining: 1
end
