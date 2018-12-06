defmodule Broadway do
  @moduledoc ~S"""
  Broadway is a concurrent, multi-stage event processing tool that aims to streamline data processing pipelines.
  It allows developers to consume data efficiently from different sources, such as Amazon SQS, RabbitMQ and others.

  ## Built-in features

    - Back-pressure
    - Batching
    - Rate-limiting (TODO)
    - Partitioning (TODO)
    - Statistics/Metrics (TODO)
    - Back-off (TODO)
    - Fault tolerance (WIP)

  ## The Broadway Behaviour

  In order to use Broadway, you need to:

    1. Define your pipeline configuration
    2. Define a module implementing the Broadway behaviour

  ### Example

  ```elixir
  {:ok, pid} =
    Broadway.start_link(MyBroadway, %{},
      name: MyBroadwayExample,
      producers: [[module: Counter, stages: 1]],
      processors: [stages: 2],
      publishers: [
        sqs: [stages: 2, batch_size: 10],
        s3: [stages: 1, batch_size: 10]
      ]
    )
  ```

  The configuration above defines a pipeline with:
  - 1 producer
  - 2 processors
  - 1 publisher named `:sqs` with 2 consumers
  - 1 publisher named `:s3` with 1 consumer

  Here is how this pipeline cound be represented:

  ```asciidoc
                       [producer_1]
                           / \
                          /   \
                         /     \
                        /       \
               [processor_1] [processor_2]   <- ideal for CPU bounded tasks
                        /\     /\
                       /  \   /  \
                      /    \ /    \
                     /      x      \
                    /      / \      \
                   /      /   \      \
                  /      /     \      \
             [batcher_sqs]    [batcher_s3]
                  /\                  \
                 /  \                  \
                /    \                  \
               /      \                  \
   [consumer_sqs_1] [consumer_sqs_2]  [consumer_s3_1]   <- publish results (usually IO bounded tasks)
  ```

  And here is how an implementation of the Broadway behaviour would look like:

  ```elixir
    defmodule MyBroadway do
      use Broadway

      def handle_message(%Message{data: data} = message, _) when is_odd(data) do
        new_message =
          message
          |> update_data(&process_data/1)
          |> put_publisher(:sqs)}

        {:ok, new_message}
      end

      def handle_message(%Message{data: data} = message, _) do
        new_message =
          message
          |> update_data(&process_data/1)
          |> put_publisher(:s3)}

        {:ok, new_message}
      end

      def handle_batch(:sqs, batch, _) do
        {successful, failed} = send_messages_to_sqs(batch.messages)
        {:ack, successful: successful, failed: failed}
      end

      def handle_batch(:s3, batch, _) do
        {successful, failed} = send_messages_to_s3(batch.messages)
        {:ack, successful: successful, failed: failed}
      end

      defp process_data(data) do
        # Do some calculations, generate a JSON representation, etc.
      end

      defp send_messages_to_sqs() do
        ...
      end

      defp send_messages_to_s3() do
        ...
      end
    end
  ```

  ## Configuration

  In order to set up how the pipeline created by Broadway is going to behave, you need to specify
  the blueprint of the pipeline. You can do this by passing a set of configuration options to
  `Broadway.start_link/3`. Each component of the pipeline has its own set of options. Some of them
  are specific to Broadway and some will be directly forwarded to the underlying GenStage layer.

  ### Options

  `:name` - Required. Used for name registration. All processes/stages created will be named using this value as prefix.

  `:producers` - Required. Defines a list of producers. Each producer can define the following options:

    * `:module` - Required. A module defining a GenStage producer. Pay attention that this producer must generate messages
    that are `Broadway.Message` structs.
    * `:arg` - Required. The argument that will be passed to the `init/1` callback of the producer.
    * `:stages` - Optional. The number of stages that will be created by Broadway. Use this option to control the concurrency level
    of each set of producers. The default value is `1`.

  `:processors` - Optional. Defines a list of options that apply to all processors. The options are:

    * `:stages` - Optional. The number of stages that will be created by Broadway. Use this option to control the concurrency level
    of the processors. The default value is `:erlang.system_info(:schedulers_online) * 2`.
    * `:min_demand` - Optional. Set the minimum demand of all processors stages. Default value is `2`.
    * `:max_demand` - Optional. Set the maximum demand of all processors stages. Default value is `4`.

  `:publishers` - Required. Defines a list of publishers. Each publisher can define the following options:

    * `:stages` - Optional. The number of stages that will be created by Broadway. Use this option to control the concurrency level
    of the publishers. Note that this only sets the numbers of consumers for each prublisher group, not the number of batchers. The
    number of batchers will always be one for each publisher key defined. The default value is `1`.
    * `:batch_size` - Optional. The size of the generated batches. Default value is `100`.
    * `:batch_timeout` - Optional. The time, in milliseconds, that the batcher waits before flushing the list of messages.
    When this timeout is reached, a new batch is generated and sent downstream, no matter if the `:batch_size` has
    been reached or not. Default value is `1000` (1 second).
    * `:min_demand` - Optional. Set the minimum demand for the producer. Default value is `2`.
    * `:max_demand` - Optional. Set the maximum demand for the producer. Default value is `4`.

  ## Batching

  In order to batch and publish messages using different publishers, you need to specify them
  in the configuration. For instance, the following configuration will define 2 different publishers, each
  one with its own set of options:

  ```elixir
  Broadway.start_link(MyBroadway, %{},
      publishers: [
        publisher1: [batch_size: 10, batch_timeout: 200, stages: 20],
        publisher2: [batch_size: 50, batch_timeout: 500, stages: 30],
      ]
    ...
  )
  ```

  If no publisher is specified, Broadway will create a single group of publishers called `:default`.

  ## General Architecture

  Broadway's architecture is built on top GenStage. That means we structure our processing units as
  independent stages that are responsible for one individual task in the pipeline. By implementing the
  Broadway bahaviour we define a GenServer process that creates and owns our pipeline.

  ### The Pipeline Model

  ```asciidoc
                                          [producers]   <- pulls data from SQS, RabbitMQ, etc.
                                               |
                                               |   (demand dispatcher)
                                               |
          handle_message/3 runs here ->   [processors]
                                              / \
                                             /   \   (partition dispatcher)
                                            /     \
                                      [batcher]   [batcher]   <- one for each publisher key
                                          |           |
                                          |           |   (demand dispatcher)
                                          |           |
       handle_batch/3 runs here ->   [consumers]  [consumers]
  ```

  ### Internal Stages

  - `Broadway.Producer` - A wrapper around the actual producer defined by the user. It will serve as the source of the pipeline.
  - `Broadway.Processor` - This is where messages are processed, e.g. do calculations, convert data into a custom json format etc. Here is where the code from handle_message/3 runs.
  - `Broadway.Batcher` - Creates batches of messages based on the publisher's key. One Batcher for each key will be created.
  - `Broadway.Consumer` - This is where the code from handle_batch/3 runs.

  ## Fault Tolerance

  Broadway was designed to provide fault tolerance out of the box. Each layer of the pipeline is independently supervised, so
  crashes in one part of the pipeline will have minimum effect in other parts.

  ```asciidoc
                                      [Broadway GenServer]
                                              |
                                              |
                                              |
                                [Broadway Pipeline Supervisor]
                                    /   (:one_for_all)      \
                                   /           |             \
                                  /            |              \
                                 /             |               \
                                /              |                \
                               /               |                 \
                [ProducerSupervisor]  [ProcessorSupervisor]    [PublisherSupervisor]
                  (:one_for_one)         (:one_for_one)          (:one_for_one)
                       / \                    / \                /            \
                      /   \                  /   \              /              \
                     /     \                /     \            /                \
                    /       \              /       \          /                  \
              [Producer_1]  ...    [Processor_1]  ...  [BatcherConsumerSuperv_1]  ...
                                                            (:one_for_one)
                                                                /  \
                                                               /    \
                                                              /      \
                                                        [Batcher] [Supervisor]
                                                                  (:one_for_one)
                                                                        |
                                                                    [Consumer]
  ```
  """

  use GenServer, shutdown: :infinity

  alias Broadway.{Producer, Processor, Batcher, Consumer, Message, BatchInfo}

  @doc """
  Invoked to handle/process indiviual messages sent from a producer.

  This is the place to do any kind of processing with the incoming message, e.g., transform the
  data into another data structure, call specific business logic to do calculations. Basically,
  any CPU bounded task that runs against a single message should be processed here.

    * `message` is the message to be processed.
    * `context` is the user defined data structure passed to `start_link/3`.

  In order to update the data after processing, use the `update_data/2` function. This way
  the new message can be properly forwared and handled by the publisher:

  ```elixir
  def handle_message(%Message{data: data} = message, _) do
    new_message =
      update_data(message, fn data ->
        do_calculation_and_returns_the_new_data(data)
      end)

    {:ok, new_message}
  end
  ```

  In case more than one publisher have been defined in the configuration, you need
  to specify which of them the resulting message will be forwarded to. You can do this by calling
  `put_publisher/2` and returning the new updated message:

  ```elixir
  def handle_message(%Message{data: data} = message, _) do
    # Do whatever you need with the data
    ...

    new_message = put_publisher(new_message, :s3)

    {:ok, new_message}
  end
  ```

  """
  @callback handle_message(message :: Message.t(), context :: any) ::
              {:ok, message :: Message.t()}

  @doc """
  Invoked to handle generated batches.

    * `publisher` is the key that defined the publisher. All messages will be grouped as batches and then forwarded to
    this callback based on this key. This value can be set in the `handle_message/2` callback using `put_publisher/2`.
    * `messages` is the list of messages of the incoming batch.
    * `bach_info` is a struct containing extra information about the incoming batch.
    * `context` is the user defined data structure passed to `start_link/3`.
  """
  @callback handle_batch(
              publisher :: atom,
              messages :: [Message.t()],
              batch_info :: BatchInfo.t(),
              context :: any
            ) :: {:ack, successful: [Message.t()], failed: [Message.t()]}

  defmodule State do
    @moduledoc false
    defstruct name: nil,
              module: nil,
              processors_config: [],
              producers_config: [],
              publishers_config: [],
              context: nil,
              supervisor_pid: nil
  end

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts, module: __CALLER__.module] do
      @behaviour Broadway

      @doc false
      def child_spec(arg) do
        default = %{
          id: unquote(module),
          start: {Broadway, :start_link, [__MODULE__, %{}, arg]}
        }

        Supervisor.child_spec(default, unquote(Macro.escape(opts)))
      end

      defoverridable child_spec: 1
    end
  end

  @doc """
  Starts a `Broadway` process linked to the current process.

    * `module` is the module implementing the `Broadway` behaviour.
    * `context` is an immutable user defined data structure that will be passed to `handle_message/2` and `handle_batch/4`.

  ## Options

    * `:name` - Required. This option is used for name registration. All processes/stages created will be named using this value as prefix.
    * `:processors` - Optional. Defines a single set of options for all processors. See the "Configuration" section for more details.
    * `:producers` - Required. Defines a list of producers. Each one with its own set of options. See the "Configuration" section for more details.
    * `:publishers` - Required. Defines a list of publishers. Each one with its own set of options. See the "Configuration" section for more details.

  """
  def start_link(module, context, opts) do
    GenServer.start_link(__MODULE__, {module, context, opts}, opts)
  end

  @doc false
  def init({module, context, opts}) do
    Process.flag(:trap_exit, true)
    state = init_state(module, context, opts)
    {:ok, supervisor_pid} = start_supervisor(state)

    {:ok, %State{state | supervisor_pid: supervisor_pid}}
  end

  def handle_info({:EXIT, _, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_reason, %State{supervisor_pid: supervisor_pid}) do
    ref = Process.monitor(supervisor_pid)
    Process.exit(supervisor_pid, :shutdown)

    receive do
      {:DOWN, ^ref, _, _, _} ->
        :ok
    end
  end

  defp init_state(module, context, opts) do
    broadway_name = Keyword.fetch!(opts, :name)
    producers_config = Keyword.fetch!(opts, :producers) |> normalize_producers_config()
    publishers_config = Keyword.get(opts, :publishers) |> normalize_publishers_config()
    processors_config = Keyword.get(opts, :processors) |> normalize_processors_config()

    %State{
      name: broadway_name,
      module: module,
      processors_config: processors_config,
      producers_config: producers_config,
      publishers_config: publishers_config,
      context: context
    }
  end

  defp start_supervisor(%State{name: broadway_name} = state) do
    supervisor_name = Module.concat(broadway_name, "Supervisor")
    {producers_names, producers_specs} = build_producers_specs(state)
    {processors_names, processors_specs} = build_processors_specs(state, producers_names)

    children = [
      build_producer_supervisor_spec(producers_specs, broadway_name),
      build_processor_supervisor_spec(processors_specs, broadway_name),
      build_publisher_supervisor_spec(
        build_batchers_consumers_supervisors_specs(state, processors_names),
        broadway_name
      )
    ]

    Supervisor.start_link(children, name: supervisor_name, strategy: :one_for_one)
  end

  defp build_producers_specs(state) do
    %State{
      name: broadway_name,
      producers_config: producers_config
    } = state

    [{key, producer_config} | other_producers] = producers_config

    if Enum.any?(other_producers) do
      raise "Only one set of producers is allowed for now"
    end

    mod = Keyword.fetch!(producer_config, :module)
    args = Keyword.fetch!(producer_config, :arg)
    n_producers = Keyword.get(producer_config, :stages, 1)

    init_acc = %{names: [], specs: []}

    producers =
      Enum.reduce(1..n_producers, init_acc, fn index, acc ->
        name = process_name(broadway_name, "Producer_#{key}", index, n_producers)
        opts = [name: name]

        spec =
          Supervisor.child_spec(
            %{start: {Producer, :start_link, [[module: mod, args: args], opts]}},
            id: make_ref()
          )

        %{acc | names: [name | acc.names], specs: [spec | acc.specs]}
      end)

    {Enum.reverse(producers.names), Enum.reverse(producers.specs)}
  end

  defp build_processors_specs(state, producers) do
    %State{
      name: broadway_name,
      module: module,
      processors_config: processors_config,
      context: context,
      publishers_config: publishers_config
    } = state

    n_processors = Keyword.fetch!(processors_config, :stages)

    init_acc = %{names: [], specs: []}

    processors =
      Enum.reduce(1..n_processors, init_acc, fn index, acc ->
        args = [
          publishers_config: publishers_config,
          processors_config: processors_config,
          module: module,
          context: context,
          producers: producers
        ]

        name = process_name(broadway_name, "Processor", index, n_processors)
        opts = [name: name]
        spec = Supervisor.child_spec({Processor, [args, opts]}, id: make_ref())

        %{acc | names: [name | acc.names], specs: [spec | acc.specs]}
      end)

    {Enum.reverse(processors.names), Enum.reverse(processors.specs)}
  end

  defp build_batchers_consumers_supervisors_specs(state, processors) do
    %State{
      name: broadway_name,
      module: module,
      context: context,
      publishers_config: publishers_config
    } = state

    for {key, _} = config <- publishers_config do
      {batcher, batcher_spec} = build_batcher_spec(broadway_name, config, processors)

      consumers_specs = build_consumers_specs(broadway_name, module, context, config, batcher)

      children = [
        batcher_spec,
        build_consumer_supervisor_spec(consumers_specs, broadway_name, key, batcher)
      ]

      build_batcher_consumer_supervisor_spec(children, broadway_name, key)
    end
  end

  defp build_batcher_spec(broadway_name, publisher_config, processors) do
    {key, options} = publisher_config
    batcher = process_name(broadway_name, "Batcher", key)
    opts = [name: batcher]

    spec =
      Supervisor.child_spec(
        {Batcher, [options ++ [publisher_key: key, processors: processors], opts]},
        id: make_ref()
      )

    {batcher, spec}
  end

  defp build_consumers_specs(broadway_name, module, context, publisher_config, batcher) do
    {key, options} = publisher_config
    n_consumers = Keyword.get(options, :stages, 1)

    for index <- 1..n_consumers do
      args = [
        module: module,
        context: context,
        batcher: batcher
      ]

      name = process_name(broadway_name, "Consumer_#{key}", index, n_consumers)
      opts = [name: name]

      Supervisor.child_spec(
        {Consumer, [args, opts]},
        id: make_ref()
      )
    end
  end

  defp normalize_processors_config(nil) do
    [stages: :erlang.system_info(:schedulers_online) * 2]
  end

  defp normalize_processors_config(config) do
    config
  end

  defp normalize_publishers_config(nil) do
    [{:default, []}]
  end

  defp normalize_publishers_config(publishers) do
    Enum.map(publishers, fn
      publisher when is_atom(publisher) -> {publisher, []}
      publisher -> publisher
    end)
  end

  defp normalize_producers_config(producers) do
    if length(producers) > 1 && !Enum.all?(producers, &is_tuple/1) do
      raise "Multiple producers must be named"
    end

    Enum.map(producers, fn
      producer when is_list(producer) -> {:default, producer}
      producer -> producer
    end)
  end

  defp process_name(prefix, type, key) do
    :"#{prefix}.#{type}_#{key}"
  end

  defp process_name(prefix, type, index, max) do
    index
    |> to_string()
    |> String.pad_leading(String.length("#{max}"), "0")
    |> (&:"#{prefix}.#{type}_#{&1}").()
  end

  defp build_producer_supervisor_spec(children, prefix) do
    build_supervisor_spec(children, :"#{prefix}.ProducerSupervisor")
  end

  defp build_processor_supervisor_spec(children, prefix) do
    build_supervisor_spec(children, :"#{prefix}.ProcessorSupervisor")
  end

  defp build_publisher_supervisor_spec(children, prefix) do
    build_supervisor_spec(children, :"#{prefix}.PublisherSupervisor")
  end

  defp build_batcher_consumer_supervisor_spec(children, prefix, key) do
    build_supervisor_spec(children, :"#{prefix}.BatcherConsumerSupervisor_#{key}")
  end

  defp build_consumer_supervisor_spec(children, prefix, key, batcher) do
    build_supervisor_spec(children, :"#{prefix}.ConsumerSupervisor_#{key}", batcher: batcher)
  end

  defp build_supervisor_spec(children, name, extra_opts \\ []) do
    opts = [name: name, strategy: :one_for_one]

    %{
      id: make_ref(),
      start: {Supervisor, :start_link, [children, opts ++ extra_opts]},
      type: :supervisor
    }
  end
end
