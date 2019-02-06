defmodule Broadway do
  @moduledoc ~S"""
  Broadway is a concurrent, multi-stage event processing tool
  that aims to streamline data processing pipelines.

  It allows developers to consume data efficiently from different
  sources, such as Amazon SQS, RabbitMQ and others.

  ## Built-in features

    * Back-pressure
    * Batching
    * Fault tolerance through restarts
    * Clean shutdown (TODO)
    * Rate-limiting (TODO)
    * Partitioning (TODO)
    * Statistics/Metrics (TODO)
    * Back-off (TODO)

  ## The Broadway Behaviour

  In order to use Broadway, you need to:

    1. Define your pipeline configuration
    2. Define a module implementing the Broadway behaviour

  ### Example

  Like any other process-based behaviour, you can start
  your Broadway process by defining a module that invokes
  `use Broadway` and has a `start_link` function:

      defmodule MyBroadway do
        use Broadway

        def start_link(_opts) do
          Broadway.start_link(MyBroadway,
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
        end

        ...callbacks...
      end

  The configuration above defines a pipeline with:

    * 1 producer
    * 2 processors
    * 1 publisher named `:sqs` with 2 consumers
    * 1 publisher named `:s3` with 1 consumer

  Here is how this pipeline would be represented:

  ```asciidoc
                       [producer_1]
                           / \
                          /   \
                         /     \
                        /       \
               [processor_1] [processor_2]   <- process each message
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
   [consumer_sqs_1] [consumer_sqs_2]  [consumer_s3_1] <- process in batches
  ```

  When using Broadway, you need to implement two callbacks:
  `c:handle_message/2`, invoked by the processor for each message,
  and `c:handle_batch/4`, invoked by consumers with each batch.
  Here is how those callbacks would be implemented:

      defmodule MyBroadway do
        use Broadway
        alias Broadway.Message

        ...start_link...

        @impl true
        def handle_message(%Message{data: data} = message, _) when is_odd(data) do
          message
          |> Message.update_data(&process_data/1)
          |> Message.put_publisher(:sqs)
        end

        def handle_message(%Message{data: data} = message, _context) do
          message
          |> Message.update_data(&process_data/1)
          |> Message.put_publisher(:s3)
        end

        @impl true
        def handle_batch(:sqs, messages, _batch_info, _context) do
          # Send batch of messages to SQS
        end

        def handle_batch(:s3, messages, _batch_info, _context) do
          # Send batch of messages to S3
        end

        defp process_data(data) do
          # Do some calculations, generate a JSON representation, etc.
        end
      end

  The publishers usually do the job of publishing the processing
  results elsewhere, although that's not strictly required. For
  example, results could be processed and published per message
  on the `c:handle_message/2` callback too. Publishers are also
  responsible to inform when a message has not been successfully
  published. You can mark a message as :failed using
  `Broadway.Message.failed/2` in either `c:handle_message/2`
  or `c:handle_batch/4`. This information will be sent back to
  the producer which will correctly acknowledge the message.

  ## General Architecture

  Broadway's architecture is built on top GenStage. That means we structure
  our processing units as independent stages that are responsible for one
  individual task in the pipeline. By implementing the `Broadway` behaviour
  we define a `GenServer` process that wraps a `Supervisor` to manage and
  own our pipeline.

  ### The Pipeline Model

  ```asciidoc
                                     [producers]   <- pulls data from SQS, RabbitMQ, etc.
                                          |
                                          |   (demand dispatcher)
                                          |
     handle_message/2 runs here ->   [processors]
                                         / \
                                        /   \   (partition dispatcher)
                                       /     \
                                 [batcher]   [batcher]   <- one for each publisher key
                                     |           |
                                     |           |   (demand dispatcher)
                                     |           |
  handle_batch/4 runs here ->   [consumers]  [consumers]
  ```

  ### Internal Stages

    * `Broadway.Producer` - A wrapper around the actual producer defined by
      the user. It serves as the source of the pipeline.
    * `Broadway.Processor` - This is where messages are processed, e.g. do
      calculations, convert data into a custom json format etc. Here is where
      the code from `handle_message/2` runs.
    * `Broadway.Batcher` - Creates batches of messages based on the
      publisher's key. One Batcher for each key will be created.
    * `Broadway.Consumer` - This is where the code from `handle_batch/4` runs.

  ### Fault-tolerance

  Broadway was designed always go back to a working state in case
  of failures thanks to the use of supervisors. Our supervision tree
  is designed as follows:

  ```asciidoc
                          [Broadway GenServer]
                                  |
                                  |
                                  |
                    [Broadway Pipeline Supervisor]
                        /   (:rest_for_one)     \
                       /           |             \
                      /            |              \
                     /             |               \
                    /              |                \
                   /               |                 \
    [ProducerSupervisor]  [ProcessorSupervisor]    [PublisherSupervisor]
      (:one_for_one)        (:one_for_all)           (:one_for_one)
           / \                    / \                /            \
          /   \                  /   \              /              \
         /     \                /     \            /                \
        /       \              /       \          /                  \
  [Producer_1]  ...    [Processor_1]  ...  [BatcherConsumerSuperv_1]  ...
                                              (:rest_for_one)
                                                    /  \
                                                   /    \
                                                  /      \
                                            [Batcher] [Supervisor]
                                                      (:one_for_all)
                                                            |
                                                        [Consumer]
  ```

  Another part of Broadway fault-tolerance comes from the fact the
  callbacks are stateless, which allows us to provide back-off in
  processors and publishers out of the box.

  Finally, Broadway guarantees proper shutdown of the supervision
  tree, making sure that processes only terminate after all of the
  currently fetched messages are processed and published (consumer).
  """

  alias Broadway.{BatchInfo, Message, Options, Server}

  @doc """
  Invoked to handle/process indiviual messages sent from a producer.

  This is the place to do any kind of processing with the incoming message,
  e.g., transform the data into another data structure, call specific business
  logic to do calculations. Basically, any CPU bounded task that runs against
  a single message should be processed here.

    * `message` is the message to be processed.
    * `context` is the user defined data structure passed to `start_link/3`.

  In order to update the data after processing, use the `update_data/2` function.
  This way the new message can be properly forwared and handled by the publisher:

      @impl true
      def handle_message(message, _) do
        message
        |> update_data(&do_calculation_and_returns_the_new_data/1)
      end

  In case more than one publisher have been defined in the configuration,
  you need to specify which of them the resulting message will be forwarded
  to. You can do this by calling `put_publisher/2` and returning the new
  updated message:

      @impl true
      def handle_message(message, _) do
        # Do whatever you need with the data
        ...

        message
        |> put_publisher(:s3)
      end

  """
  @callback handle_message(message :: Message.t(), context :: any) :: Message.t()

  @doc """
  Invoked to handle generated batches.

    * `publisher` is the key that defined the publisher. All messages
      will be grouped as batches and then forwarded to this callback
      based on this key. This value can be set in the `handle_message/2`
      callback using `put_publisher/2`.
    * `messages` is the list of messages of the incoming batch.
    * `bach_info` is a struct containing extra information about the incoming batch.
    * `context` is the user defined data structure passed to `start_link/3`.

  """
  @callback handle_batch(
              publisher :: atom,
              messages :: [Message.t()],
              batch_info :: BatchInfo.t(),
              context :: any
            ) :: [Message.t()]

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

  ## Options

  In order to set up how the pipeline created by Broadway,
  you need to specify the blueprint of the pipeline. You can
  do this by passing a set of options to `start_link/3`.
  Each component of the pipeline has its own set of options.

  The broadway options are:

    * `:name` - Required. Used for name registration. All processes/stages
      created will be named using this value as prefix.

    * `:producers` - Required. Defines a keyword list of named producers
      where the key is an atom as identifier and the value is another
      keyword list of options. See "Producers options" section below.

    * `:processors` - Required. A keyword list of options that apply to
      all processors. See "Processors options" section below.

    * `:publishers` - Required. Defines a keyword list of named publishers
      where the key is an atom as identifier and the value is another
      keyword list of options. See "Consumers options" section below.

    * `context` is an immutable user defined data structure that will
      be passed to `handle_message/2` and `handle_batch/4`.

  ### Producers options

  The producer options are:

    * `:module` - Required. A module defining a GenStage producer.
      Pay attention that this producer must emit events that are
      `Broadway.Message` structs.
    * `:arg` - Required. The argument that will be passed to the
      `init/1` callback of the producer.
    * `:stages` - Optional. The number of stages that will be
      created by Broadway. Use this option to control the concurrency
      level of each set of producers. The default value is `1`.

  ### Processors options

  The processors options are:

    * `:stages` - Optional. The number of stages that will be created
      by Broadway. Use this option to control the concurrency level
      of the processors. The default value is `System.schedulers_online() * 2`.
    * `:min_demand` - Optional. Set the minimum demand of all processors
      stages. Default value is `5`.
    * `:max_demand` - Optional. Set the maximum demand of all processors
      stages. Default value is `10`.

  ### Publishers options

    * `:stages` - Optional. The number of stages that will be created by
      Broadway. Use this option to control the concurrency level of the
      publishers. Note that this only sets the numbers of consumers for
      each prublisher group, not the number of batchers. The number of
      batchers will always be one for each publisher key defined.
      The default value is `1`.

    * `:batch_size` - Optional. The size of the generated batches.
      Default value is `100`.

    * `:batch_timeout` - Optional. The time, in milliseconds, that the
      batcher waits before flushing the list of messages. When this timeout
      is reached, a new batch is generated and sent downstream, no matter
      if the `:batch_size` has been reached or not. Default value is `1000`
      (1 second).

  """
  def start_link(module, opts) do
    case Options.validate(opts, configuration_spec()) do
      {:error, message} ->
        raise ArgumentError, "invalid configuration given to Broadway.start_link/3, " <> message

      {:ok, opts} ->
        Server.start_link(module, opts)
    end
  end

  defp configuration_spec() do
    [
      name: [required: true, type: :atom],
      context: [type: :any, default: :context_not_set],
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
