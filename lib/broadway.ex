defmodule Broadway do
  @moduledoc ~S"""
  Broadway is a concurrent, multi-stage tool for building
  data ingestion and data processing pipelines.

  It allows developers to consume data efficiently from different
  sources, such as Amazon SQS, Apache Kafka, Google Cloud PubSub,
  RabbitMQ and others.

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

    * Custom failure handling - Broadway provides a `c:handle_failed/2` callback
      where developers can outline custom code to handle errors. For example,
      if they want to move messages to another queue for further processing.

    * Dynamic batching - Broadway allows developers to batch messages based on
      custom criteria. For example, if your pipeline needs to build
      batches based on the `user_id`, email address, etc, it can be done
      by calling `Broadway.Message.put_batch_key/2`.

    * Ordering and Partitioning - Broadway allows developers to partition
      messages across workers, guaranteeing messages within the same partition
      are processed in order. For example, if you want to guarantee all events
      tied to a given `user_id` are processed in order and not concurrently,
      you can set the `:partition_by` option. See "Ordering and partitioning".

    * Rate limiting: Broadway allows developers to rate limit all producers in
      a single node by a given number of messages in a time period, allowing
      developers to easily work sources or sinks that cannot cope with a high
      number of requests. See the ":rate_limiting" option for producers in
      `start_link/2`.

    * Metrics - Broadway uses the `:telemetry` library for instrumentation,
      see "Telemetry" section below for more information.

    * Back-off (TODO)

  ## The Broadway Behaviour

  In order to use Broadway, you need to:

    1. Define your pipeline configuration
    2. Define a module implementing the Broadway behaviour

  ### Example

  Broadway is a process-based behaviour, and you begin by
  defining a module that invokes `use Broadway`. Processes
  defined by these modules will often be started by a
  supervisor, and so a `start_link/1` function is frequently
  also defined but not strictly necessary.

      defmodule MyBroadway do
        use Broadway

        def start_link(_opts) do
          Broadway.start_link(MyBroadway,
            name: MyBroadwayExample,
            producer: [
              module: {Counter, []},
              concurrency: 1
            ],
            processors: [
              default: [concurrency: 2]
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

  Adding your pipeline to your supervision tree in this way
  calls the default `child_spec/1` function that is generated
  when `use Broadway` is invoked. If you would like to customize
  the child spec passed to the supervisor, you can override the
  `child_spec/1` function in your module or explicitly pass a
  child spec to the supervisor when adding it to your supervision tree.

  The configuration above defines a pipeline with:

    * One producer
    * Two processors

  Here is how this pipeline would be represented:

  ```asciidoc
                       [producer_1]
                           / \
                          /   \
                         /     \
                        /       \
               [processor_1] [processor_2]   <- process each message
  ```

  After the pipeline is defined, you need to implement the `c:handle_message/3`
  callback which will be invoked by processors for each message.

  `c:handle_message/3` receives every message as a `Broadway.Message`
  struct and it must return an updated message.

  ## Batching

  Depending on the scenario, you may want to group processed messages as
  batches before publishing your data. This is common and especially
  important when working with services like AWS S3 and SQS that provide a
  specific API for sending and retrieving batches. This can drastically
  increase throughput and consequently improve the overall performance of
  your pipeline.

  To create batches, define the `batchers` configuration option:

      defmodule MyBroadway do
        use Broadway

        def start_link(_opts) do
          Broadway.start_link(MyBroadway,
            name: MyBroadwayExample,
            producer: [
              module: {Counter, []},
              concurrency: 1
            ],
            processors: [
              default: [concurrency: 2]
            ],
            batchers: [
              sqs: [concurrency: 2, batch_size: 10],
              s3: [concurrency: 1, batch_size: 10]
            ]
          )
        end

        ...callbacks...
      end

  The configuration above defines a pipeline with:

    * One producer
    * Two processors
    * One batcher named `:sqs` with two batch processors
    * One batcher named `:s3` with one batch processor

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
     [batch_sqs_1] [batch_sqs_2]    [batch_s3_1] <- process each batch
  ```

  Additionally, define the `c:handle_batch/4` callback,
  which batch processors invoke for each batch. You can then
  call `Broadway.Message.put_batcher/2` inside `c:handle_message/3` to
  control which batcher the message should go to.

  The batcher receives processed messages and creates batches
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
          # Send batch of successful messages as ACKs to SQS
          # This tells SQS that this list of messages were successfully processed
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
  is also emitted. If your Broadway module also defines the
  `c:handle_failed/2` callback, that callback will be invoked with
  all the failed messages before they get acknowledged.

  Note however, that `Broadway` does not provide any sort of retries
  out of the box. This is left completely as a responsibility of the
  producer. For instance, if you are using Amazon SQS, the default
  behaviour is to retry unacknowledged messages after a user-defined
  timeout. If you don't want unacknowledged messages to be retried,
  is your responsibility to configure a dead-letter queue as target
  for those messages.

  ## Testing

  Many producers receive data from external systems and hitting the network
  is usually undesirable when running the tests. One way to solve this issue
  would be to not start Broadway pipeline in tests. Another way would be to use
  a different producer in tests, one that doesn't do anything, and that is
  exactly what `Broadway.DummyProducer` is for. If the dummy producer doesn't
  produce any work, how to test that the pipeline is correct? For that, Broadway
  ships with a `test_messages/2` function.

  With `test_messages/2`, you can push some sample data into the pipeline and
  receive a process message when the pipeline acknowledges the data you have
  pushed has been processed. This is very useful as a synchronization mechanism.
  Because many pipelines end-up working with side-effects, you can use the
  test message acknowledgment to guarantee the message has been processed and
  therefore side-effects should be visible.

  For example, if you have a pipeline named `MyApp.Broadway` that writes to
  the database on every message, you could test it as:

      # Push 3 messages with the data field set to 1, 2, and 3 respectively
      ref = Broadway.test_messages(MyApp.Broadway, [1, 2, 3])

      # Assert that the messages have been consumed
      assert_receive {:ack, ^ref, [_, _, _] = _successful, failed}, 1000

      # Now assert the database side-effects
      ...

  Also note how we have increased the `assert_receive` timeout to 1000ms.
  The default timeout is 100ms, which may not be enough for some pipelines.
  You may also increase the `assert_receive` timeout for the whole suite
  in your `test/test_helper.exs`:

      ExUnit.configure(assert_receive_timeout: 2000)

  When testing pipelines with batchers there are additional considerations.
  By default, the batch is only delivered when either its size or its timeout
  has been reached, but that is often impractical for testing, you may not
  necessarily want to send a lot of data or wait a lot of time for the batch
  to flush. For this reason, when using `test_messages/2`, the messages have
  their `:batch_mode` set to `:flush`, causing the batch to be delivered
  immediately, without waiting for the batch size or the timeout.

  ## Ordering and partitioning

  By default, Broadway processes all messages and batches concurrently,
  which means ordering is not guaranteed. Some producers may impose some
  ordering (for instance, Apache Kafka), but if the ordering comes from a
  business requirement, you will have to impose the ordering yourself.
  This can be done with the `:partition_by` option, which enforces that
  messages with a given property are always forwarded to the same stage.

  In order to provide partitioning throughout the whole pipeline, just
  set `:partition_by` at the root of your configuration:

      defmodule MyBroadway do
        use Broadway

        def start_link(_opts) do
          Broadway.start_link(MyBroadway,
            name: MyBroadwayExample,
            producer: [
              module: {Counter, []},
              concurrency: 1
            ],
            processors: [
              default: [concurrency: 2]
            ],
            batchers: [
              sqs: [concurrency: 2, batch_size: 10],
              s3: [concurrency: 1, batch_size: 10]
            ],
            partition_by: &partition/1
          )
        end

        defp partition(msg) do
          msg.data.user_id
        end

  In the example above, we are partitioning the pipeline by `user_id`.
  This means any message with the same `user_id` will be handled by
  the same processor and batch processor.

  The `partition` function must return a non-negative integer,
  starting at zero, which is routed to a stage by using the `remainder`
  option.

  If the data you want to partition by is not an integer, you can
  explicitly hash it by calling `:erlang.phash2/1`. However, note
  that `hash` does not guarantee an equal distribution of events
  across partitions. So some partitions may be more overloaded than
  others, slowing down the whole pipeline.

  In the example above, we have set the same partition for all
  processors and batchers. You can also specify the `:partition_by`
  function for each "processor" and "batcher" individually.

  Finally, beware of the error semantics when using partitioning.
  If you require ordering and a message fails, the partition will
  continue processing messages. Depending on the type of processing,
  the end result may be inconsistent. If your producer supports
  retrying, the failed message may be retried later, also out of
  order. Those issues happens regardless of Broadway and solutions
  to said problems almost always need to be addressed outside of
  Broadway too.

  ## Telemetry

  Broadway currently exposes following Telemetry events:

    * `[:broadway, :processor, :start]` - Dispatched by a Broadway processor when
       the internal GenStage `handle_events/3` callback is invoked

      * Measurement: `%{time: System.monotonic_time}`
      * Metadata: `%{name: atom, messages: [Broadway.Message.t]}`

    * `[:broadway, :processor, :stop]` -  Dispatched by a Broadway processor when
      the internal GenStage `handle_events/3` callback has completed processing
      all the individual messages

      * Measurement: `%{time: System.monotonic_time, duration: native_time}`

      * Metadata:

        ```
        %{
          name: atom,
          successful_messages_to_ack: [Broadway.Message.t],
          successful_messages_to_forward: [Broadway.Message.t],
          failed_messages: [Broadway.Message.t]
        }
        ```

    * `[:broadway, :processor, :message, :start]` - Dispatched by a Broadway processor
      before your `c:handle_message/3` callback is invoked

      * Measurement: `%{time: System.monotonic_time}`

      * Metadata:

        ```
        %{
          processor_key: atom,
          name: atom,
          message: Broadway.Message.t
        }
        ```

    * `[:broadway, :processor, :message, :stop]` - Dispatched by a Broadway processor
      after your `c:handle_message/3` callback has returned

      * Measurement: `%{time: System.monotonic_time, duration: native_time}`

      * Metadata:

        ```
        %{
          processor_key: atom,
          name: atom,
          message: Broadway.Message.t,
          updated_message: Broadway.Message.t
        }
        ```

    * `[:broadway, :processor, :message, :failure]` - Dispatched by a Broadway processor
      if your `c:handle_message/3` callback encounters an error

      * Measurement: `%{time: System.monotonic_time, duration: native_time}`

      * Metadata:

        ```
        %{
          processor_key: atom,
          name: atom,
          message: Broadway.Message.t,
          kind: kind,
          reason: reason,
          stacktrace: stacktrace
        }
        ```

    * `[:broadway, :consumer, :start]` - Dispatched by a Broadway consumer before your
      `c:handle_batch/4` callback is invoked

      * Measurement: `%{time: System.monotonic_time}`
      * Metadata:

        ```
        %{
          name: atom,
          messages: [Broadway.Message.t],
          batch_info: Broadway.BatchInfo.t
        }
        ```

    * `[:broadway, :consumer, :stop]` - Dispatched by a Broadway consumer after your
      `c:handle_batch/4` callback has returned

      * Measurement: `%{time: System.monotonic_time, duration: native_time}`

      * Metadata:

        ```
        %{
          name: atom,
          successful_messages: [Broadway.Message.t],
          failed_messages: [Broadway.Message.t],
          batch_info: Broadway.BatchInfo.t
        }
        ```

    * `[:broadway, :batcher, :start]` - Dispatched by a Broadway batcher before
      handling events

      * Measurement: `%{time: System.monotonic_time}`
      * Metadata: `%{name: atom, events: [{Broadway.Message.t}]}`

    * `[:broadway, :batcher, :stop]` - Dispatched by a Broadway batcher after
      handling events

      * Measurement: `%{time: System.monotonic_time, duration: native_time}`
      * Metadata: `%{name: atom}`
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
  steps of the pipeline. This callback also traps exits, so failures due to broken
  links between processes do not automatically cascade.
  """
  @callback handle_message(processor :: atom, message :: Message.t(), context :: term) ::
              Message.t()

  @doc """
  Invoked to handle generated batches.

  It expects:

    * `batcher` is the key that defined the batcher. This value can be
      set in the `handle_message/3` callback using `Broadway.Message.put_batcher/2`.
    * `messages` is the list of `Broadway.Message` structs in the incoming batch.
    * `batch_info` is a `Broadway.BatchInfo` struct containing extra information
      about the incoming batch.
    * `context` is the user defined data structure passed to `start_link/2`.

  It must return an updated list of messages. All messages received must be returned,
  otherwise an error will be logged. All messages after this step will be acknowledged
  acccording to their status.

  In case of errors in this callback, the error will be logged and the whole
  batch will be failed. This callback also traps exits, so failures due to broken
  links between processes do not automatically cascade.
  """
  @callback handle_batch(
              batcher :: atom,
              messages :: [Message.t()],
              batch_info :: BatchInfo.t(),
              context :: term
            ) :: [Message.t()]

  @doc """
  Invoked for failed messages (if defined).

  It expects:

    * `messages` is the list of messages that failed. If a message is failed in
      `c:handle_message/3`, this will be a list with a single message in it. If
      some messages are failed in `c:handle_batch/4`, this will be the list of
      failed messages.

    * `context` is the user-defined data structure passed to `start_link/2`.

  This callback must return the same messages given to it, possibly updated.
  For example, you could update the message data or use `Broadway.Message.configure_ack/2`
  in a centralized place to configure how to ack the message based on the failure
  reason.

  This callback is optional. If present, it's called **before** the messages
  are acknowledged according to the producer. This gives you a chance to do something
  with the message before it's acknowledged, such as storing it in an external
  persistence layer or similar.

  This callback is also invoked if `c:handle_message/3` or `c:handle_batch/4`
  crash or raise an error. If this callback crashes or raises an error,
  the messages are failed internally by Broadway to avoid crashing the process.
  """
  @doc since: "0.5.0"
  @callback handle_failed(messages :: [Message.t()], context :: term) :: [Message.t()]

  @optional_callbacks handle_batch: 4, handle_failed: 2

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts, module: __CALLER__.module] do
      @behaviour Broadway

      @doc false
      def child_spec(arg) do
        default = %{
          id: unquote(module),
          start: {__MODULE__, :start_link, [arg]},
          shutdown: :infinity
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

  In order to set up how the pipeline created by Broadway should work,
  you need to specify the blueprint of the pipeline. You can
  do this by passing a set of options to `start_link/2`.
  Each component of the pipeline has its own set of options.

  The broadway options are:

    * `:name` - Required. Used for name registration. All processes/stages
      created will be named using this value as prefix.

    * `:producer` - Required. A keyword list of options. See "Producers
      options" section below. Only a single producer is allowed.

    * `:processors` - Required. A keyword list of named processors
      where the key is an atom as identifier and the value is another
      keyword list of options. See "Processors options" section below.
      Currently only a single processor is allowed.

    * `:batchers` - Optional. A keyword list of named batchers
      where the key is an atom as identifier and the value is another
      keyword list of options. See "Batchers options" section below.

    * `:context` - Optional. A user defined data structure that will
      be passed to `handle_message/3` and `handle_batch/4`.

    * `:shutdown` - Optional. The time in milliseconds given for Broadway to
      gracefully shutdown without discarding events. Defaults to `30_000`(ms).

    * `:resubscribe_interval` - Optional. The interval in milliseconds that
      processors wait until they resubscribe to a failed producers. Defaults
      to `100`(ms).

    * `:partition_by` - Optional. A function that controls how data is
      partitioned across all processors and batchers. It receives a
      `Broadway.Message` and it must return a non-negative integer,
      starting with zero, that will be mapped to one of the existing
      processors. See "Ordering and Partitioning" in the module docs
      for more information.

    * `:hibernate_after` - Optional. If a process does not receive any
      message within this interval, it will hibernate, compacting memory.
      Applies to producers, processors, and batchers. Defaults to `15_000`(ms).

    * `:spawn_opt` - Optional. Low-level options given when starting a
      process. Applies to producers, processors, and batchers.
      See `erlang:spawn_opt/2` for more information.

  ### Producers options

  The producer options are:

    * `:module` - Required. A tuple representing a GenStage producer.
      The tuple format should be `{mod, arg}`, where `mod` is the module
      that implements the GenStage behaviour and `arg` the argument that will
      be passed to the `init/1` callback of the producer. Pay attention that
      this producer must emit events that are `Broadway.Message` structs.
      It's recommended that `arg` is a keyword list. In fact, if `arg` is
      a keyword list, a `:broadway` option is injected into such keyword list
      containing the configuration for the complete Broadway topology with the
      addition of an `:index` key, telling the index of the producer in its
      supervision tree (starting from 0). This allows a features such having
      even producers connect to some server while odd producers connect to
      another.

    * `:concurrency` - Optional. The number of concurrent producers that
      will be started by Broadway. Use this option to control the concurrency
      level of each set of producers. The default value is `1`.

    * `:transformer` - Optional. A tuple representing a transformer
       that translates a produced GenStage event into a `%Broadway.Message{}`.
       The tuple format should be `{mod, fun, opts}` and the function should have
       the following spec `(event :: term, opts :: term) :: Broadway.Message.t`
       This function must be used sparingly and exclusively to convert regular
       messages into `Broadway.Message`. That's because a failure in the
       `:transformer` callback will cause the whole producer to terminate,
       possibly leaving unacknowledged messages along the way.

    * `:rate_limiting` - Optional. A list of options to enable and configure
      rate limiting for producing. If this option is present, rate limiting is
      enabled, otherwise it isn't. Rate limiting refers to the rate at which
      producers will forward messages to the rest of the pipeline. The rate
      limiting is applied to and shared by all producers within the time limit.
      The following options are supported:

        * `:allowed_messages` - Required. An integer that describes how many
          messages are allowed in the specified interval.

        * `:interval` - Required. An integer that describes the interval
         (in milliseconds) during which the number of allowed messages is
         allowed. If the producer produces more than `allowed_messages`
         in `interval`, only `allowed_messages` will be published until
         the end of `interval`, and then more messages will be published.

    * `:hibernate_after` - Optional. Overrides the top-level `:hibernate_after`.

    * `:spawn_opt` - Optional. Overrides the top-level `:spawn_opt`.

  ### Processors options

  The processors options are:

    * `:concurrency` - Optional. The number of concurrent process that will
      be started by Broadway. Use this option to control the concurrency level
      of the processors. The default value is `System.schedulers_online() * 2`.

    * `:min_demand` - Optional. Set the minimum demand of all processors
      stages. Default value is `5`.

    * `:max_demand` - Optional. Set the maximum demand of all processors
      stages. Default value is `10`.

    * `:partition_by` - Optional. Overrides the top-level `:partition_by`.

    * `:hibernate_after` - Optional. Overrides the top-level `:hibernate_after`.

    * `:spawn_opt` - Optional. Overrides the top-level `:spawn_opt`.

  ### Batchers options

    * `:concurrency` - Optional. The number of concurrent batch processors
      that will be started by Broadway. Use this option to control the
      concurrency level. Note that this only sets the numbers of batch
      processors for each batcher group, not the number of batchers.
      The number of batchers will always be one for each batcher key
      defined. The default value is `1`.

    * `:batch_size` - Optional. The size of the generated batches.
      Default value is `100`.

    * `:batch_timeout` - Optional. The time, in milliseconds, that the
      batcher waits before flushing the list of messages. When this timeout
      is reached, a new batch is generated and sent downstream, no matter
      if the `:batch_size` has been reached or not. Default value is `1000`
      (1 second).

    * `:partition_by` - Optional. Overrides the top-level `:partition_by`.

    * `:hibernate_after` - Optional. Overrides the top-level `:hibernate_after`.

    * `:spawn_opt` - Optional. Overrides the top-level `:spawn_opt`.

  """
  def start_link(module, opts) do
    opts =
      case Keyword.pop(opts, :producers) do
        {nil, opts} ->
          opts

        {[{_key, producer}], opts} ->
          IO.warn("""
          :producers key in Broadway.start_link is deprecated.

          Instead of:

              producers: [
                default: [
                  ...
                ]
              ]

          Do:

              producer: [
                ...
              ]
          """)

          Keyword.put(opts, :producer, producer)
      end

    case Options.validate(opts, configuration_spec()) do
      {:error, message} ->
        raise ArgumentError, "invalid configuration given to Broadway.start_link/2, " <> message

      {:ok, opts} ->
        opts =
          opts
          |> carry_over_one(:producer, [:hibernate_after, :spawn_opt])
          |> carry_over_many(:processors, [:partition_by, :hibernate_after, :spawn_opt])
          |> carry_over_many(:batchers, [:partition_by, :hibernate_after, :spawn_opt])

        Server.start_link(module, opts)
    end
  end

  defp carry_over_one(opts, key, keys) do
    update_in(opts[key], fn value -> Keyword.merge(Keyword.take(opts, keys), value) end)
  end

  defp carry_over_many(opts, key, keys) do
    update_in(opts[key], fn list ->
      defaults = Keyword.take(opts, keys)
      for {k, v} <- list, do: {k, Keyword.merge(defaults, v)}
    end)
  end

  @doc """
  Returns the names of producers.

  ## Examples

      iex> Broadway.producer_names(MyBroadway)
      [MyBroadway.Producer_0, MyBroadway.Producer_1, ..., MyBroadway.Producer_7]

  """
  @spec producer_names(GenServer.server()) :: [atom()]
  def producer_names(broadway) do
    Server.producer_names(broadway)
  end

  @doc """
  Sends a list of `Broadway.Message`s to the Broadway pipeline.

  The producer is randomly chosen among all sets of producers/stages.
  This is used to send out of band data to a Broadway pipeline.
  """
  @spec push_messages(GenServer.server(), messages :: [Message.t()]) :: :ok
  def push_messages(broadway, messages) when is_list(messages) do
    broadway
    |> producer_names()
    |> Enum.random()
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

  See "Testing" section in module documentation for more information.

  ## Options

  * `:batch_mode` - when set to `:flush`, the batch the message is
    in is immediately delivered. When set to `:bulk`, batch is
    delivered when its size or timeout is reached. Defaults to `:flush`.

  * `:metadata` - optionally a map of additional fields to add to the
    message. This can be used, for example, when testing
    `BroadwayRabbitMQ.Producer`.

  ## Examples

  For example, in your tests, you may do:

      ref = Broadway.test_messages(broadway, [1, 2, 3])
      assert_receive {:ack, ^ref, successful, failed}
      assert length(successful) == 3
      assert length(failed) == 0

  """
  @spec test_messages(GenServer.server(), data :: [term], opts :: Keyword.t()) :: reference
  def test_messages(broadway, data, opts \\ []) when is_list(data) and is_list(opts) do
    batch_mode = Keyword.get(opts, :batch_mode, :flush)
    metadata = Map.new(Keyword.get(opts, :metadata, []))

    ref = make_ref()
    ack = {Broadway.CallerAcknowledger, {self(), ref}, :ok}

    messages =
      Enum.map(
        data,
        &%Message{data: &1, acknowledger: ack, batch_mode: batch_mode, metadata: metadata}
      )

    :ok = push_messages(broadway, messages)
    ref
  end

  @doc """
  Gets the current values used for the producer rate limiting of the given pipeline.

  Returns `{:ok, info}` if rate limiting is enabled for the given pipeline or
  `{:error, reason}` if the given pipeline doesn't have rate limiting enabled.

  The returned info is a map with the following keys:

    * `:interval`
    * `:allowed_messages`

  See the `:rate_limiting` options in the module documentation for more information.

  ## Examples

      Broadway.get_rate_limiting(broadway)
      #=> {:ok, %{allowed_messages: 2000, interval: 1000}}

  """
  @doc since: "0.6.0"
  @spec get_rate_limiting(GenServer.server()) ::
          {:ok, rate_limiting_info} | {:error, :rate_limiting_not_enabled}
        when rate_limiting_info: %{
               required(:interval) => non_neg_integer(),
               required(:allowed_messages) => non_neg_integer()
             }
  def get_rate_limiting(broadway) do
    with {:ok, rate_limiter_name} <- Server.get_rate_limiter(broadway) do
      {:ok, Broadway.RateLimiter.get_rate_limiting(rate_limiter_name)}
    end
  end

  @doc """
  Updates the producer rate limiting of the given pipeline at runtime.

  Supports the following options (see the `:rate_limiting` options in the module
  documentation for more information):

    * `:allowed_messages`
    * `:interval`

  Returns an `{:error, reason}` tuple if the given `broadway` pipeline doesn't
  have rate limiting enabled.

  ## Examples

      Broadway.update_rate_limiting(broadway, allowed_messages: 100)

  """
  @doc since: "0.6.0"
  @spec update_rate_limiting(GenServer.server(), opts :: Keyword.t()) ::
          :ok | {:error, :rate_limiting_not_enabled}
  def update_rate_limiting(broadway, opts) when is_list(opts) do
    spec = [
      allowed_messages: [type: :pos_integer],
      interval: [type: :pos_integer]
    ]

    with {:validate_opts, {:ok, opts}} <- {:validate_opts, Options.validate(opts, spec)},
         {:get_name, {:ok, rate_limiter_name}} <- {:get_name, Server.get_rate_limiter(broadway)} do
      Broadway.RateLimiter.update_rate_limiting(rate_limiter_name, opts)
    else
      {:validate_opts, {:error, message}} ->
        raise ArgumentError, "invalid options, " <> message

      {:get_name, {:error, reason}} ->
        {:error, reason}
    end
  end

  defp configuration_spec() do
    [
      name: [required: true, type: :atom],
      shutdown: [type: :pos_integer, default: 30000],
      max_restarts: [type: :non_neg_integer, default: 3],
      max_seconds: [type: :pos_integer, default: 5],
      resubscribe_interval: [type: :non_neg_integer, default: 100],
      context: [type: :any, default: :context_not_set],
      producer: [
        required: true,
        type: :non_empty_keyword_list,
        keys: [
          module: [required: true, type: :mod_arg],
          stages: [
            type: :pos_integer,
            deprecated: "Use :concurrency instead",
            rename_to: :concurrency
          ],
          concurrency: [type: :pos_integer, default: 1],
          transformer: [type: :mfa, default: nil],
          spawn_opt: [type: :keyword_list],
          hibernate_after: [type: :pos_integer],
          rate_limiting: [
            type: :non_empty_keyword_list,
            keys: [
              allowed_messages: [required: true, type: :pos_integer],
              interval: [required: true, type: :pos_integer]
            ]
          ]
        ]
      ],
      processors: [
        required: true,
        type: :non_empty_keyword_list,
        keys: [
          *: [
            stages: [
              type: :pos_integer,
              deprecated: "Use :concurrency instead",
              rename_to: :concurrency
            ],
            concurrency: [type: :pos_integer, default: System.schedulers_online() * 2],
            min_demand: [type: :non_neg_integer],
            max_demand: [type: :non_neg_integer, default: 10],
            partition_by: [type: {:fun, 1}],
            spawn_opt: [type: :keyword_list],
            hibernate_after: [type: :pos_integer]
          ]
        ]
      ],
      batchers: [
        default: [],
        type: :keyword_list,
        keys: [
          *: [
            stages: [
              type: :pos_integer,
              deprecated: "Use :concurrency instead",
              rename_to: :concurrency
            ],
            concurrency: [type: :pos_integer, default: 1],
            batch_size: [type: :pos_integer, default: 100],
            batch_timeout: [type: :pos_integer, default: 1000],
            partition_by: [type: {:fun, 1}],
            spawn_opt: [type: :keyword_list],
            hibernate_after: [type: :pos_integer]
          ]
        ]
      ],
      partition_by: [type: {:fun, 1}],
      spawn_opt: [type: :keyword_list],
      hibernate_after: [type: :pos_integer, default: 15_000]
    ]
  end
end
