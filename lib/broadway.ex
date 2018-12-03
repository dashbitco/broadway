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

  ```Elixir
  {:ok, pid} =
    Broadway.start_link(MyBroadway, %{},
      name: MyBroadwayExample,
      producers: [[module: Counter]],
      processors: [stages: 2],
      publishers: [
        sqs: [stages: 2, batch_size: 10],
        s3: [stages: 1, batch_size: 10]
      ]
    )
  ```

  In the configuration above we've defined that our pipeline will have:
  - One producer (Counter)
  - Two processors
  - Two publishers. One with 2 consumers and the other one with only 1 consumer.

  Here is how this pipeline cound be represented:

  ```
                           [producer]  (Counter)
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
                     /\                   \
                    /  \                   \
                   /    \                   \
                  /      \                   \
      [consumer_sqs_1] [consumer_sqs_2]   [consumer_s3_1]   <- usually publish results (IO bounded tasks)
  ```

  And here is how an implementation of the Broadway behaviour would look like:

  ```Elixir
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
          |> update_data(&process_message/1)
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

  ## General Architecture

  Broadway's architecture is built on top GenStage. That means we structure our processing units as
  independent stages that are responsible for one individual task in the pipeline.

  ### The Pipeline Model

  ```
                                            [producer]   (pulls data from SQS, RabbitMQ, etc.)
                                                |
                                                |   (demand dispatcher)
                                                |
          handle_message/3 runs here ->   [processors]
                                              / \
                                             /   \   (partition dispatcher)
                                            /     \
                                      [batcher]   [batcher]   (one for each publisher key)
                                        |           |
                                        |           |   (demand dispatcher)
                                        |           |
      handle_batch/3 runs here ->   [consumers]  [consumers]
  ```

  ### Stages

  - **Broadway** _\[User defined\]_ - Not actually a Stage. This is the parent GenServer that creates and owns the pipeline (Must implement the Broadway bahaviour).
  - **Broadway.Producer** _\[User defined\]_ - This is a ordinary GenStage producer that will serve as the source of the pipeline.
  - **Broadway.Processor** _\[Internal\]_ - This is where you usually process your messages, e.g. do calculations, convert data into a custom json format etc. Here is where the code from handle_message/3 will run.
  - **Broadway.Batcher** _\[Internal\]_ - Create batches of messages based on the publisher's key. One Batcher for each key will be created.
  - **Broadway.Consumer** _\[Internal\]_ - Here is where the code from handle_batch/3 will run.

  ## Fault Tolerance

  Broadway was design to provide fault tolerance out of the box. Each layer of the pipeline is independently supervised, so
  crashes in one part of the pipeline will have minimum effect in other parts.

  ```
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

  `message` is the message to be processed and `context` is the user defined data structure
  passed to `start_link/3`.
  """
  @callback handle_message(message :: Message.t(), context :: any) ::
              {:ok, message :: Message.t()}

  @doc """
  Invoked to handle generated batches.

  `publisher` is the key that defines how the messages will be grouped as batches and then forwarded to
  the set of consumers that handle the batches with the same key. This value can be set in `handle_message/2`
  using `put_publisher/2`.
  The `messages` are the list of messages grouped together as a batch for a specific key.
  The `bach_info` is a struct containing information about the generated batch.
  The `context` is the user defined data structure passed to `start_link/3`.
  """
  @callback handle_batch(
              publisher :: atom,
              messages :: [Message.t()],
              batch_info :: BatchInfo.t(),
              context :: any
            ) :: {:ack, successful: [Message.t()], failed: [Message.t()]}

  defmodule State do
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

  `module` is the module implementing the `Broadway` behaviour, `context` is a user defined
  data structure that will be passed to `handle_message/2` and `handle_batch/4`.

  ## Options

    * `:name` - required parameter used for name registration. All processes/stages created will be named using this value as prefix.
    * `:processors` - defines the processors configuration.
    * `:producers` - defines the producers configuration.
    * `:publishers` - defines the publishers configuration.

  """
  def start_link(module, context, opts) do
    GenServer.start_link(__MODULE__, {module, context, opts}, opts)
  end

  def init({module, context, opts}) do
    Process.flag(:trap_exit, true)
    state = init_state(module, context, opts)
    {:ok, supervisor_pid} = start_supervisor(state)

    {:ok, %State{state | supervisor_pid: supervisor_pid}}
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

  def start_supervisor(%State{name: broadway_name} = state) do
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

  def handle_info({:EXIT, _, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
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

  def terminate(_reason, %State{supervisor_pid: supervisor_pid}) do
    ref = Process.monitor(supervisor_pid)
    Process.exit(supervisor_pid, :shutdown)

    receive do
      {:DOWN, ^ref, _, _, _} ->
        :ok
    end
  end
end
