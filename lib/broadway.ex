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
      producers: [
        default: [module: Counter, stages: 1]
      ],
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

  `:processors` - Required. Defines a list of options that apply to all processors. The options are:

    * `:stages` - Optional. The number of stages that will be created by Broadway. Use this option to control the concurrency level
    of the processors. The default value is `System.schedulers_online() * 2`.
    * `:min_demand` - Optional. Set the minimum demand of all processors stages. Default value is `5`.
    * `:max_demand` - Optional. Set the maximum demand of all processors stages. Default value is `10`.

  `:publishers` - Required. Defines a list of publishers. Each publisher can define the following options:

    * `:stages` - Optional. The number of stages that will be created by Broadway. Use this option to control the concurrency level
    of the publishers. Note that this only sets the numbers of consumers for each prublisher group, not the number of batchers. The
    number of batchers will always be one for each publisher key defined. The default value is `1`.
    * `:batch_size` - Optional. The size of the generated batches. Default value is `100`.
    * `:batch_timeout` - Optional. The time, in milliseconds, that the batcher waits before flushing the list of messages.
    When this timeout is reached, a new batch is generated and sent downstream, no matter if the `:batch_size` has
    been reached or not. Default value is `1000` (1 second).

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

  alias Broadway.{BatchInfo, Message, Options, Server}

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

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts, module: __CALLER__.module] do
      @behaviour Broadway

      @doc false
      def child_spec(arg) do
        default = %{
          id: unquote(module),
          start: {__MODULE__, :start_link, [arg]},
          type: :supervisor
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
    * `:processors` - Required. Defines a single set of options for all processors. See the "Configuration" section for more details.
    * `:producers` - Required. Defines a list of producers. Each one with its own set of options. See the "Configuration" section for more details.
    * `:publishers` - Required. Defines a list of publishers. Each one with its own set of options. See the "Configuration" section for more details.

  """
  def start_link(module, context, opts) do
    case Options.validate(opts, configuration_spec()) do
      {:error, message} ->
        raise ArgumentError, "invalid configuration given to Broadway.start_link/3, " <> message

      {:ok, opts} ->
        Server.start_link(module, context, opts)
    end
  end

  defp configuration_spec() do
    [
      name: [required: true, type: :atom],
      producers: [
        required: true,
        type: :keyword_list,
        keys: [
          *: [
            module: [required: true, type: :atom],
            arg: [required: true],
            stages: [type: :pos_integer, default: 1]
          ]
        ]
      ],
      processors: [
        required: true,
        type: :keyword_list,
        keys: [
          stages: [type: :pos_integer, default: System.schedulers_online() * 2],
          min_demand: [type: :non_neg_integer],
          max_demand: [type: :non_neg_integer, default: 10]
        ]
      ],
      publishers: [
        required: true,
        type: :keyword_list,
        keys: [
          *: [
            stages: [type: :pos_integer, default: 1],
            batch_size: [type: :pos_integer, default: 100],
            batch_timeout: [type: :pos_integer, default: 1000]
          ]
        ]
      ]
    ]
  end
end
