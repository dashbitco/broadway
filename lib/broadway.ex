defmodule Broadway do
  @moduledoc ~S"""
  Broadway is a concurrent, multi-stage tool for building
  data ingestion and data processing pipelines.

  It allows developers to consume data efficiently from different
  sources, such as Amazon SQS, RabbitMQ and others.

  ## Built-in features

    * Back-pressure - by relying on `GenStage`, we only get the amount
      of events necessary from upstream sources, never flooding the
      pipeline.

    * Automatic acknowledgements - Broadway automatically acknowledges
      messages at the end of the pipeline or in case of errors.

    * Batching - Broadway provides built-in batching, allowing you to
      group messages either by size and/or by time. This is important
      in systems such as Amazon SQS, where batching is the most efficient
      way to consume messages, both in terms of time and cost.

    * Fault tolerance with minimal data loss - Broadway pipelines are
      carefully designed to minimize data loss. Producers are isolated
      from the rest of the pipeline and automatically resubscribed to
      in case of failures. On the other hand, user callbacks are stateless,
      allowing us to handle any errors locally. Finally, in face of any
      unforeseen bug, we restart only downstream components, avoiding
      data loss.

    * Graceful shutdown - Broadway integrates with the VM to provide graceful
      shutdown. By starting Broadway as part of your supervision tree, it will
      guarantee all events are flushed once the VM shuts down.

    * Built-in testing - Broadway ships with a built-in test API, making it
      easy to push test messages through the pipeline and making sure the
      event was properly processed.

    * Partitioning - Broadway allows developers to batch messages based on
      dynamic partitions. For example, if your pipeline needs to build
      batches based on the `user_id`, email address, etc, it can be done
      by calling `Broadway.Message.put_batch_key/2`.

    * Rate-limiting (TODO)
    * Statistics/Metrics (TODO)
    * Back-off (TODO)

  ## The Broadway Behaviour

  In order to use Broadway, you need to:

    1. Define your pipeline configuration
    2. Define a module implementing the Broadway behaviour

  ### Example

  Like any other process-based behaviour, you can start your Broadway
  process by defining a module that invokes `use Broadway` and has a
  `start_link` function:

      defmodule MyBroadway do
        use Broadway

        def start_link(_opts) do
          Broadway.start_link(MyBroadway,
            name: MyBroadwayExample,
            producers: [
              default: [
                module: {Counter, []},
                stages: 1
              ]
            ],
            processors: [
              default: [stages: 2]
            ]
          )
        end

        ...callbacks...
      end

  Then add your Broadway pipeline to your supervision tree
  (usually in `lib/my_app/application.ex`):

      children = [
        {MyBroadway, []}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)

  The configuration above defines a pipeline with:

    * 1 producer
    * 2 processors

  Here is how this pipeline would be represented:

  ```asciidoc
                       [producer_1]
                           / \
                          /   \
                         /     \
                        /       \
               [processor_1] [processor_2]   <- process each message
  ```

  After the pipeline is defined, you need to implement `c:handle_message/3`,
  which will be invoked by processors for each message.

  `c:handle_message/3` receives every message as a `Broadway.Message`
  struct and it must return an updated message.

  ## Batching

  Depending on the scenario, you may want to group processed messages as
  batches before publishing your data. This is common and especially
  important when working with services like AWS S3 and SQS that provide
  specific API for sending and retrieving batches. This can drastically
  increase throughput and consequently improve the overall performance of
  your pipeline.

  In order to create batches you need to define the `batchers` option in the
  configuration:

      defmodule MyBroadway do
        use Broadway

        def start_link(_opts) do
          Broadway.start_link(MyBroadway,
            name: MyBroadwayExample,
            producers: [
              default: [
                module: {Counter, []},
                stages: 1
              ]
            ],
            processors: [
              default: [stages: 2]
            ],
            batchers: [
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
    * 1 batcher named `:sqs` with 2 consumers
    * 1 batcher named `:s3` with 1 consumer

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
   [consumer_sqs_1] [consumer_sqs_2]  [consumer_s3_1] <- process each batch
  ```

  Additionally, you'll need to define the `c:handle_batch/4` callback,
  which will be invoked by consumers for each batch. You can then invoke
  `Broadway.Message.put_batcher/2` inside `c:handle_message/3` to control
  to which batcher the message should go to.

  The batcher will receive the processed messages and create batches
  specified by the `batch_size` and `batch_timeout` configuration. The
  goal is to create a batch with at most `batch_size` entries within
  `batch_timeout` milliseconds. Each message goes into a particular batch,
  controlled by calling `Broadway.Message.put_batch_key/2` in
  `c:handle_message/3`. Once a batch is created, it is sent to a separate
  process that will call `c:handle_batch/4`, passing the batcher, the
  batch itself (i.e. a list of messages), a `Broadway.BatchInfo` struct
  and the Broadway context.

  For example, imagine your producer generates integers as `data`.
  You want to route the odd integers to SQS and the even ones to
  S3. Your pipeline would look like this:

      defmodule MyBroadway do
        use Broadway
        import Integer

        alias Broadway.Message

        ...start_link...

        @impl true
        def handle_message(_, %Message{data: data} = message, _) when is_odd(data) do
          message
          |> Message.update_data(&process_data/1)
          |> Message.put_batcher(:sqs)
        end

        def handle_message(_, %Message{data: data} = message, _) when is_even(data) do
          message
          |> Message.update_data(&process_data/1)
          |> Message.put_batcher(:s3)
        end

        defp process_data(data) do
          # Do some calculations, generate a JSON representation, etc.
        end

        @impl true
        def handle_batch(:sqs, messages, _batch_info, _context) do
          # Send batch of messages to SQS
        end

        def handle_batch(:s3, messages, _batch_info, _context) do
          # Send batch of messages to S3
        end
      end

  See the callbacks documentation for more information on the
  arguments given to each callback and their expected return types.

  Now you are ready to get started. See the `start_link/2` function
  for a complete reference on the arguments and options allowed.

  Also makes sure to check out GUIDES in the documentation sidebar
  for more examples, how tos and more.

  ## Acknowledgements and failures

  At the end of the pipeline, messages are automatically acknowledged.

  If there are no batchers, the acknowledgement will be done by processors.
  The number of messages acknowledged, assuming the pipeline is running
  at full scale, will be `max_demand - min_demand`. Since the default values
  are 10 and 5 respectively, we will be acknowledging in groups of 5.

  If there are batchers, the acknowledgement is done by the batchers,
  using the `batch_size`.

  In case of failures, Broadway does its best to keep the failures
  contained and avoid losing messages. The failed message or batch is
  acknowledged as failed immediately. For every failure, a log report
  is also emitted.

  Note however, that `Broadway` does not provide any sort of retries
  out of the box. This is left completely as a responsibility of the
  producer. For instance, if you are using Amazon SQS, the default
  behaviour is to retry unacknowledged messages after a user-defined
  timeout. If you don't want unacknowledged messages to be retried,
  is your responsibility to configure a dead-letter queue as target
  for those messages.

  ## Testing

  Testing Broadway pipelines can be done with `test_messages/2`.
  With `test_messages/2`, you can push some sample data into the
  pipeline and receive a process message when the pipeline
  acknowledges the data you have pushed has been processed.
  This is very useful as a synchronization mechanism. Because
  many pipelines end-up working with side-effects, you can use
  the test message acknowledgment to guarantee the message has
  been processed and therefore side-effects should be visible.

  For example, if you have a pipeline named `MyApp.Broadway` that
  writes to the database on every message, you could test it as:

      # Push 3 messages with the data field set to 1, 2, and 3 respectively
      ref = Broadway.test_messages(MyApp.Broadway, [1, 2, 3])

      # Assert that the messages have been consumed
      assert_receive {:ack, ^ref, [_, _, _] = _successful, failed}

      # Now assert the database side-effects
      ...

  Keep in mind that multiple acknowledgement messages may be sent.
  For example, if the batcher in the example above has size of 2,
  then two batches would be created and therefore two ack messages
  would be sent. Similarly, if any of the messages fail when
  processed, an acknowledgement of their failure may be sent early
  on. On the positive side, if you always push just a single test
  message, then there is always one acknowledgment.
  """

  alias Broadway.{BatchInfo, Message, Options, Server, Producer}

  @doc """
  Invoked to handle/process individual messages sent from a producer.

  It receives:

    * `processor`  is the key that defined the processor.
    * `message` is the `Broadway.Message` struct to be processed.
    * `context` is the user defined data structure passed to `start_link/2`.

  And it must return the (potentially) updated `Broadway.Message` struct.

  This is the place to do any kind of processing with the incoming message,
  e.g., transform the data into another data structure, call specific business
  logic to do calculations. Basically, any CPU bounded task that runs against
  a single message should be processed here.

  In order to update the data after processing, use the
  `Broadway.Message.update_data/2` function. This way the new message can be
  properly forwarded and handled by the batcher:

      @impl true
      def handle_message(_, message, _) do
        message
        |> update_data(&do_calculation_and_returns_the_new_data/1)
      end

  In case more than one batcher have been defined in the configuration,
  you need to specify which of them the resulting message will be forwarded
  to. You can do this by calling `put_batcher/2` and returning the new
  updated message:

      @impl true
      def handle_message(_, message, _) do
        # Do whatever you need with the data
        ...

        message
        |> put_batcher(:s3)
      end

  Any message that has not been explicitly failed will be forwarded to the next
  step in the pipeline. If there are no extra steps, it will be automatically
  acknowledged.

  In case of errors in this callback, the error will be logged and that particular
  message will be immediately acknowledged as failed, not proceeding to the next
  steps of the pipeline.
  """
  @callback handle_message(processor :: atom, message :: Message.t(), context :: term) ::
              Message.t()

  @doc """
  Invoked to handle generated batches.

  It expects:

    * `batcher` is the key that defined the batcher. This value can be
      set in the `handle_message/3` callback using `Broadway.Message.put_batcher/2`.
    * `messages` is the list of `Broadway.Message` structs of the incoming batch.
    * `batch_info` is a `Broadway.BatchInfo` struct containing extra information
      about the incoming batch.
    * `context` is the user defined data structure passed to `start_link/2`.

  It must return a list of batches. Any message in the batch that has not been
  explicitly failed will be considered successful and automatically acknowledged.

  In case of errors in this callback, the error will be logged and the whole
  batch will be failed.
  """
  @callback handle_batch(
              batcher :: atom,
              messages :: [Message.t()],
              batch_info :: BatchInfo.t(),
              context :: term
            ) :: [Message.t()]

  @optional_callbacks handle_batch: 4

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
  do this by passing a set of options to `start_link/2`.
  Each component of the pipeline has its own set of options.

  The broadway options are:

    * `:name` - Required. Used for name registration. All processes/stages
      created will be named using this value as prefix.

    * `:producers` - Required. Defines a keyword list of named producers
      where the key is an atom as identifier and the value is another
      keyword list of options. See "Producers options" section below.
      Currently only a single producer is allowed.

    * `:processors` - Required. A keyword list of named processors
      where the key is an atom as identifier and the value is another
      keyword list of options. See "Processors options" section below.
      Currently only a single processor is allowed.

    * `:batchers` - Optional. Defines a keyword list of named batchers
      where the key is an atom as identifier and the value is another
      keyword list of options. See "Batchers options" section below.

    * `:context` - Optional. A user defined data structure that will
      be passed to `handle_message/3` and `handle_batch/4`.

    * `:shutdown` - Optional. The time in milliseconds given for Broadway to
      gracefully shutdown without discarding events. Defaults to `30_000`(ms).

    * `:resubscribe_interval` - The interval in milliseconds to attempt to
      subscribe to a producer after it crashes. Defaults to `100`(ms).

  ### Producers options

  The producer options are:

    * `:module` - Required. A tuple representing a GenStage producer.
      The tuple format should be `{mod, arg}`, where `mod` is the module
      that implements the GenStage behaviour and `arg` the argument that will
      be passed to the `init/1` callback of the producer. Pay attention that
      this producer must emit events that are `Broadway.Message` structs.
    * `:stages` - Optional. The number of stages that will be
      created by Broadway. Use this option to control the concurrency
      level of each set of producers. The default value is `1`.
    * `:transformer` - Optional. A tuple representing a transformer
       that translates a produced GenStage event into a `%Broadway.Message{}`.
       The tuple format should be `{mod, fun, opts}` and the function should have
       the following spec `(event :: term, opts :: term) :: Broadway.Message.t`

  ### Processors options

  The processors options are:

    * `:stages` - Optional. The number of stages that will be created
      by Broadway. Use this option to control the concurrency level
      of the processors. The default value is `System.schedulers_online() * 2`.
    * `:min_demand` - Optional. Set the minimum demand of all processors
      stages. Default value is `5`.
    * `:max_demand` - Optional. Set the maximum demand of all processors
      stages. Default value is `10`.

  ### Batchers options

    * `:stages` - Optional. The number of stages that will be created by
      Broadway. Use this option to control the concurrency level.
      Note that this only sets the numbers of consumers for
      each batcher group, not the number of batchers. The number of
      batchers will always be one for each batcher key defined.
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
        raise ArgumentError, "invalid configuration given to Broadway.start_link/2, " <> message

      {:ok, opts} ->
        Server.start_link(module, opts)
    end
  end

  @doc """
  Sends a list of `Broadway.Message`s to the Broadway pipeline.

  The producer is randomly chosen among all sets of producers/stages.
  This is used to send out of band data to a Broadway pipeline.
  """
  @spec push_messages(GenServer.server(), messages :: [Message.t()]) :: :ok
  def push_messages(broadway, messages) when is_list(messages) do
    broadway
    |> Server.get_random_producer()
    |> Producer.push_messages(messages)
  end

  @doc """
  Sends a list of data as messages to the Broadway pipeline.

  This is a convenience used mostly for testing. The given data
  is automatically wrapped in a `Broadway.Message` with
  `Broadway.CallerAcknowledger` configured to send a message
  back to the caller once the message has been fully processed.
  It uses `push_messages/2` for dispatching.

  It returns a reference that can be used to identify the ack
  messages.

  ## Examples

  For example, in your tests, you may do:

      ref = Broadway.test_messages(broadway, [1, 2, 3])
      assert_receive {:ack, ^ref, successful, failed}
      assert length(successful) == 3
      assert length(failed) == 0

  """
  @spec test_messages(GenServer.server(), data :: [term]) :: reference
  def test_messages(broadway, data) when is_list(data) do
    ref = make_ref()
    ack = {Broadway.CallerAcknowledger, {self(), ref}, :ok}
    messages = Enum.map(data, &%Message{data: &1, acknowledger: ack})
    :ok = push_messages(broadway, messages)
    ref
  end

  defp configuration_spec() do
    [
      name: [required: true, type: :atom],
      shutdown: [type: :pos_integer, default: 30000],
      max_restarts: [type: :non_neg_integer, default: 3],
      max_seconds: [type: :pos_integer, default: 5],
      resubscribe_interval: [type: :non_neg_integer, default: 100],
      context: [type: :any, default: :context_not_set],
      producers: [
        required: true,
        type: :non_empty_keyword_list,
        keys: [
          *: [
            module: [required: true, type: :mod_arg],
            stages: [type: :pos_integer, default: 1],
            transformer: [type: :mfa, default: nil]
          ]
        ]
      ],
      processors: [
        required: true,
        type: :non_empty_keyword_list,
        keys: [
          *: [
            stages: [type: :pos_integer, default: System.schedulers_online() * 2],
            min_demand: [type: :non_neg_integer],
            max_demand: [type: :non_neg_integer, default: 10]
          ]
        ]
      ],
      batchers: [
        default: [],
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
