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

    * Fault tolerance - Broadway pipelines are carefully designed to manage
      failures. Producers are isolated from the rest of the pipeline and
      automatically resubscribe to your data source in case of crashes.
      At the same time, all of your Broadway callbacks are stateless, which
      allows Broadway to handle any errors locally. This provides a stable
      foundation that play well with your producers, regardless if their
      delivery guarantees are at least once, at most once, or exactly once.

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
      you can set the `:partition_by` option. See ["Ordering and partitioning"](#module-ordering-and-partitioning).

    * Rate limiting - Broadway allows developers to rate limit all producers in
      a single node by a given number of messages in a time period, allowing
      developers to easily work sources or sinks that cannot cope with a high
      number of requests. See the `:rate_limiting` option for producers in
      `start_link/2`.

    * Metrics - Broadway uses the `:telemetry` library for instrumentation,
      see ["Telemetry"](#module-telemetry) section below for more information.

  ## The Broadway behaviour

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

  To create batches, define the `:batchers` configuration option:

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

        # ...callbacks...
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

  Additionally, you have to define the `c:handle_batch/4` callback,
  which batch processors invoke for each batch. You can then
  call `Broadway.Message.put_batcher/2` inside `c:handle_message/3` to
  control which batcher the message should go to.

  The batcher receives processed messages and creates batches
  specified by the `batch_size` and `batch_timeout` configuration. The
  goal is to create a batch with at most `batch_size` entries within
  `batch_timeout` milliseconds. Each message goes into a particular batch,
  controlled by calling `Broadway.Message.put_batch_key/2` in
  `c:handle_message/3`. Once a batch is created in the batcher, it is sent
  to a separate process (the batch processor) that will call `c:handle_batch/4`,
  passing the batcher, the batch itself (a list of messages), a `Broadway.BatchInfo`
  struct, and the Broadway context.

  For example, imagine your producer generates integers as `data`.
  You want to route the odd integers to SQS and the even ones to
  S3. Your pipeline would look like this:

      defmodule MyBroadway do
        use Broadway
        import Integer

        alias Broadway.Message

        # ...start_link...

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

  See the [callbacks documentation](#callbacks) for more information on the
  arguments given to each callback and their expected return types.

  ### The default batcher

  Once you define the `:batchers` configuration key for your Broadway pipeline,
  then **all messages get batched**. By default, unless you call
  `Broadway.Message.put_batcher/2`, messages have their batcher set to the
  `:default` batcher. If you don't define configuration for it, Broadway is going
  to raise an error.

  For example, imagine you want to batch "special" messages and handle them differently
  then all other messages. You can configure your pipeline like this:

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
              special: [concurrency: 2, batch_size: 10],
              default: [concurrency: 1, batch_size: 10]
            ]
          )
        end

        def handle_message(_, message, _) do
          if special?(message) do
            message
            |> Broadway.Message.put_batcher(:special)
          else
            message
          end
        end

        def handle_batch(:special, messages, _batch_info, _context) do
          # Handle special batch
        end

        def handle_batch(:default, messages, _batch_info, _context) do
          # Handle all other messages in batches
        end

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

  ## Producer concurrency

  Setting producer concurrency is a tradeoff between latency and internal
  queueing.

  For efficiency, you should generally limit the amount of internal queueing.
  Whenever additional messages are sitting in a busy processor's mailbox, they
  can't be delivered to another processor which may be available or become
  available first.

  One possible cause of internal queueing is multiple producers. This is because
  each processor's demand will be sent to all producers. For example, if a
  processor demands `2` messages and there are `2` producers, each producer
  will try to produce `2` messages (for example, by pulling from a queue or
  whatever the specific producer does) and give them to the processor. So the
  processor may receive `max_demand * <producer concurrency>` messages.

  Setting producer `concurrency: 1` will reduce internal queueing. This is
  likely a good choice for producers which take minimal time to produce a
  message, such as `BroadwayRabbitMQ`, which receives messages as they are
  pushed by RabbitMQ and can specify how many to prefetch.

  On the other hand, when using a producer such as `BroadwaySQS` which must
  make a network round trip to fetch from an external source, it may be better
  to use multiple producers and accept some internal queueing to avoid having
  fetch messages whenever there is new demand.

  Measure your system to decide which setting is most appropriate.

  Adding another single-producer pipeline, or another node running the
  pipeline, are other ways you may consider to increase throughput.

  ## Batcher concurrency

  If a batcher's `concurrency` is greater than `1`, Broadway will use as few of
  the batcher processes as possible at any given moment, attempting to satisfy
  the `batch_size` of one batcher process within the `batch_timeout` before
  sending messages to another.

  ## Testing

  Many producers receive data from external systems and hitting the network
  is usually undesirable when running the tests.

  For testing purposes, we recommend developers to use `Broadway.DummyProducer`.
  This producer does not produce any messages by itself and instead the
  `test_message/3` and `test_batch/3` functions should be used to publish
  messages.

  With `test_message/3`, you can push a message into the pipeline and receive
  a process message when the pipeline acknowledges the data you have pushed
  has been processed.

  Let's see an example. Imagine the following `Broadway` module:

      defmodule MyBroadway do
        use Broadway

        def start_link() do
          producer_module = Application.fetch_env!(:my_app, :producer_module)
          producer_options = Application.get_env(:my_app, :producer_options, [])

          Broadway.start_link(__MODULE__,
            name: __MODULE__,
            producer: [
              module: {producer_module, producer_options}
            ],
            processors: [
              default: []
            ],
            batchers: [
              default: [batch_size: 10]
            ]
          )
        end

        @impl true
        def handle_message(_processor, message, _context) do
          message
        end

        @impl true
        def handle_batch(_batcher, messages, _batch_info, _context) do
          messages
        end
      end

  Now in config/test.exs you could do:

      config :my_app,
        producer_module: Broadway.DummyProducer,
        producer_options: [] # change if required for your dev/prod producer

  And we can test it like this:

      defmodule MyBroadwayTest do
        use ExUnit.Case, async: true

        test "test message" do
          ref = Broadway.test_message(MyBroadway, 1)
          assert_receive {:ack, ^ref, [%{data: 1}], []}
        end
      end

  Note that at the end we received a message in the format of:

      {:ack, ^ref, successful_messages, failure_messages}

  You can use the acknowledgment to guarantee the message has been
  processed and therefore any side-effect from the pipeline should be
  visible.

  When using `test_message/3`, the message will be delivered as soon as
  possible, without waiting for the pipeline `batch_size` to be reached
  or without waiting for `batch_timeout`. This behaviour is useful to test
  and verify single messages, without imposing high timeouts to our test
  suites.

  In case you want to test multiple messages, then you need to use
  `test_batch/3`. `test_batch/3` will respect the batching configuration,
  which most likely means you need to increase your test timeouts:

      test "batch messages" do
        ref = Broadway.test_batch(MyBroadway, [1, 2, 3])
        assert_receive {:ack, ^ref, [%{data: 1}, %{data: 2}, %{data: 3}], []}, 1000
      end

  However, keep in mind that, generally speaking, there is no guarantee
  the messages will arrive in the same order that you have sent them,
  especially for large batches, as Broadway will process large batches
  concurrently and order will be lost.

  If you want to send more than one test message at once, then we recommend
  setting the `:batch_mode` to `:bulk`, especially if you want to assert how
  the code will behave with large batches. Otherwise the batcher will flush
  messages as soon as possible and in small batches.

  However, keep in mind that, regardless of the `:batch_mode` you cannot
  rely on ordering, as Broadway pipelines are inherently concurrent. For
  example, if you send those messages:

      test "multiple batch messages" do
        ref = Broadway.test_batch(MyBroadway, [1, 2, 3, 4, 5, 6, 7], batch_mode: :bulk)
        assert_receive {:ack, ^ref, [%{data: 1}], []}, 1000
      end

  ### Testing with Ecto

  If you are using Ecto in your Broadway processors and you want
  to run your tests concurrently, you need to tell Broadway to
  use the Ecto SQL Sandbox during tests. This can be done in two
  steps.

  First, when you call `test_messages/3` in your tests, include
  the `:ecto_sandbox` process in the message metadata:

      Broadway.test_message(MyApp.Pipeline, message, metadata: %{ecto_sandbox: self()})

  Now we can use Broadway telemetry callbacks to fetch the sandbox
  process and enable it inside the processor. Add to your
  `test/test_helper.exs`:

      defmodule BroadwayEctoSandbox do
        def attach(repo) do
          events = [
            [:broadway, :processor, :start],
            [:broadway, :batch_processor, :start],
          ]

          :telemetry.attach_many({__MODULE__, repo}, events, &__MODULE__.handle_event/4, %{repo: repo})
        end

        def handle_event(_event_name, _event_measurement, %{messages: messages}, %{repo: repo}) do
          with [%Broadway.Message{metadata: %{ecto_sandbox: pid}} | _] <- messages do
            Ecto.Adapters.SQL.Sandbox.allow(repo, pid, self())
          end

          :ok
        end
      end

      BroadwayEctoSandbox.attach(MyApp.Repo)

  And now you should have concurrent Broadway tests that talk to the database.

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
          Broadway.start_link(__MODULE__,
            name: __MODULE__,
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

  > #### Even partitions {: .warning}
  >
  > Broadway partitions assume an even distribution of partitions.
  > This means that, if one partition is slow, it will slow down
  > all order partitions. This implies two things:
  >
  > * Using `:partition_by` with a high level of concurrency can
  >   actually be detrimental to performance. For example, if
  >   concurrency is set to 100, you need all 100 processors to
  >   make progress at the same time.
  >
  > * Avoid using `:partition_by` with a low value of `min_demand`.
  >   For example, setting `max_demand` to 1 (which implies `min_demand`
  >   of 0), means that each processor will receive a single message
  >   and only receive further messages once all processors complete.
  >
  > When partitioning, the default values for concurrency (which is
  > equal to the number of cores) and max_demand (which is equal to
  > 10), are good starting points.

  > #### Error semantics {: .warning}
  >
  > Beware of the error semantics when using partitioning.
  > If you require messages to be processed in order and a message
  > fails, the partition will continue processing messages, which
  > may be undesired. If your producer supports retrying, the
  > failed message may be retried later, out of its original order.
  > Those issues happen regardless of Broadway and solutions to said
  > problems almost always need to be addressed outside of Broadway too.

  ## Configuration storage

  Broadway stores configuration globally in a configurable storage method.
  Broadway comes with two configuration storage options:

    * `:persistent_term` (the default)
    * `:ets`

  ### Persistent term

  This is the most efficient option for static Broadway pipeline definitions,
  as this option never deletes the Broadway configuration from storage. It's based
  on [`:persistent_term`](`:persistent_term`).

  ```elixir
  config :broadway, config_storage: :persistent_term
  ```

  The speed of storing and updating using `:persistent_term` is proportional
  to the number of already-created terms in the storage. If you are creating
  several Broadway pipelines dynamically, that may affect the persistent term
  storage performance. Furthermore, even if you are restarting the same pipeline
  but you are using different parameters each time, that will require a global
  garbage collection pass to update the `:persistent_term` configuration.
  If you are starting Broadway pipelines dynamically, you must use the `:ets`
  storage.

  ### ETS

  An [ETS](`:ets`)-backed configuration storage, useful if Broadway pipelines are
  started dynamically. To use this configuration storage option, configure the
  `:broadway` application in :your configuration

  ```elixir
  # For example, in config/config.exs:
  config :broadway, config_storage: :ets
  ```

  Using `:ets` as the config storage will allow for a dynamic number of Broadway server
  configurations to be stored and fetched without the associated performance trade-offs
  that `:persistent_term` has.

  ## Telemetry

  Broadway currently exposes following Telemetry events:

    * `[:broadway, :topology, :init]` - Dispatched when the topology for
      a Broadway pipeline is initialized. The config key in the metadata
      contains the configuration options that were provided to
      `Broadway.start_link/2`.

      * Measurement: `%{system_time: integer}`
      * Metadata: `%{supervisor_pid: pid, config: keyword}`

    * `[:broadway, :processor, :start]` - Dispatched by a Broadway processor
      before the optional `c:prepare_messages/2`

      * Measurement: `%{system_time: integer}`
      * Metadata:

        ```
        %{
          topology_name: atom,
          name: atom,
          processor_key: atom,
          index: non_neg_integer,
          messages: [Broadway.Message.t],
          telemetry_span_context: reference,
          producer: {atom, list}
        }
        ```

    * `[:broadway, :processor, :stop]` -  Dispatched by a Broadway processor
      after `c:prepare_messages/2` and after all `c:handle_message/3` callback
      has been invoked for all individual messages

      * Measurement: `%{duration: native_time}`

      * Metadata:

        ```
        %{
          topology_name: atom,
          name: atom,
          processor_key: atom,
          index: non_neg_integer,
          successful_messages_to_ack: [Broadway.Message.t],
          successful_messages_to_forward: [Broadway.Message.t],
          failed_messages: [Broadway.Message.t],
          telemetry_span_context: reference,
          producer: {atom, list}
        }
        ```

    * `[:broadway, :processor, :message, :start]` - Dispatched by a Broadway processor
      before your `c:handle_message/3` callback is invoked

      * Measurement: `%{system_time: integer}`

      * Metadata:

        ```
        %{
          processor_key: atom,
          topology_name: atom,
          name: atom,
          index: non_neg_integer,
          message: Broadway.Message.t,
          telemetry_span_context: reference
        }
        ```

    * `[:broadway, :processor, :message, :stop]` - Dispatched by a Broadway processor
      after your `c:handle_message/3` callback has returned

      * Measurement: `%{duration: native_time}`

      * Metadata:

        ```
        %{
          processor_key: atom,
          topology_name: atom,
          name: atom,
          index: non_neg_integer,
          message: Broadway.Message.t,
          telemetry_span_context: reference
        }
        ```

    * `[:broadway, :processor, :message, :exception]` - Dispatched by a Broadway processor
      if your `c:handle_message/3` callback encounters an exception

      * Measurement: `%{duration: native_time}`

      * Metadata:

        ```
        %{
          processor_key: atom,
          topology_name: atom,
          name: atom,
          index: non_neg_integer,
          message: Broadway.Message.t,
          kind: kind,
          reason: reason,
          stacktrace: stacktrace,
          telemetry_span_context: reference
        }
        ```

    * `[:broadway, :batch_processor, :start]` - Dispatched by a Broadway batch processor
      before your `c:handle_batch/4` callback is invoked

      * Measurement: `%{system_time: integer}`
      * Metadata:

        ```
        %{
          topology_name: atom,
          name: atom,
          index: non_neg_integer,
          messages: [Broadway.Message.t],
          batch_info: Broadway.BatchInfo.t,
          telemetry_span_context: reference,
          producer: {atom, list}
        }
        ```

    * `[:broadway, :batch_processor, :stop]` - Dispatched by a Broadway batch
      processor after your `c:handle_batch/4` callback has returned

      * Measurement: `%{duration: native_time}`

      * Metadata:

        ```
        %{
          topology_name: atom,
          name: atom,
          index: non_neg_integer,
          successful_messages: [Broadway.Message.t],
          failed_messages: [Broadway.Message.t],
          batch_info: Broadway.BatchInfo.t,
          telemetry_span_context: reference,
          producer: {atom, list}
        }
        ```

    * `[:broadway, :batcher, :start]` - Dispatched by a Broadway batcher before
      handling events

      * Measurement: `%{system_time: integer}`
      * Metadata:

        ```
        %{
          topology_name: atom,
          name: atom,
          batcher_key: atom,
          messages: [Broadway.Message.t],
          telemetry_span_context: reference
        }
        ```

    * `[:broadway, :batcher, :stop]` - Dispatched by a Broadway batcher after
      handling events

      * Measurement: `%{duration: native_time}`
      * Metadata:

      ```
        %{
          topology_name: atom,
          name: atom,
          batcher_key: atom,
          telemetry_span_context: reference
        }
      ```

  Most of the events follow the `:telemetry.span/3` convention for measurements.
  This means that "start" events have a `:system_time` representing the start of
  that event using `System.system_time/0`. The "stop" or "exception" events
  have the `duration` value, which is the difference in monotonic time between
  the start and stop events.

  """

  alias Broadway.{BatchInfo, Message, Topology, ConfigStorage}
  alias NimbleOptions.ValidationError

  @typedoc """
  Returned by `start_link/2`.
  """
  @type on_start() :: {:ok, pid()} | :ignore | {:error, {:already_started, pid()} | term()}
  @type name :: atom() | {:via, module(), term()}

  @doc """
  Invoked for preparing messages before handling (if defined).

  It expects:

    * `messages` is a list of `Broadway.Message` structs to be processed.
    * `context` is the user defined data structure passed to `start_link/2`.

  This is the place to prepare and preload any information that will be used
  by `c:handle_message/3`. For example, if you need to query the database,
  instead of doing it once per message, you can do it on this callback as
  a best-effort optimization.

  The length of the list of messages received by this callback is often based
  on the `min_demand`/`max_demand` configuration in the processor but ultimately
  it depends on the producer and on the frequency data arrives. A pipeline that
  receives messages rarely will most likely emit lists of length below the
  `min_demand` value. Producers which are push-based, rather than pull-based,
  such as `BroadwayRabbitMQ.Producer`, are more likely to send messages as they
  arrive (which may skip batching altogether and always be single element lists).
  In other words, this callback is simply a convenience for preparing messages,
  it does not guarantee the messages will be accumulated to a certain length.
  For effective batch processing, see `c:handle_batch/4`.

  This callback must always return all messages it receives, as
  `c:handle_message/3` is still called individually for each message afterwards.

  > #### Failed Messages {: .warning}
  >
  > Even if `c:prepare_messages/2` **fails** some messages (`Broadway.Message.failed/2`),
  > the failed messages are still passed down to `c:handle_message/3`.
  > If your pipeline wants to avoid processing messages failed in `c:prepare_messages/2`,
  > it will have to pattern match on `%Broadway.Message{status: {:failed, reason}}`
  > in its `c:handle_message/3` callback and act accordingly.
  """
  @callback prepare_messages(messages :: [Message.t()], context :: term) :: [Message.t()]

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
  according to their status.

  In case of errors in this callback, the error will be logged and the whole
  batch will be failed. This callback also traps exits, so failures due to broken
  links between processes do not automatically cascade.

  For more information on batching, see the "Batching" section in the `Broadway`
  documentation.
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

  @doc """
  Invoked to get the process name of this Broadway pipeline.

  `broadway_name` is the name given to `start_link/2` in the `:name` option. `base_name`
  is a string used by Broadway to identify different components of the pipeline
  whose name needs to be registered (such as "batcher" or "processor").

  The return value of this callback must be a process name that is valid for registration.
  See the name registration rules in the documentation for `GenServer`.

  This callback is optional. If not defined, the `broadway_name` given to `start_link/2`
  **must be an atom**: the default implementation of this callback will fail otherwise.

  ## Examples

      @impl Broadway
      def process_name({:via, module, term}, base_name) do
        {:via, module, {term, base_name}}
      end

  """
  @doc since: "1.1.0"
  @callback process_name(broadway_name :: Broadway.name(), base_name :: String.t()) ::
              Broadway.name()

  @doc """
  Invoked when items are discarded from the buffer.

  If this callback returns `true`, the default log message is emitted.
  See `c:GenStage.format_discarded/2`.

  Allows controlling or customization of the log message emitted.
  """
  @doc since: "1.2.0"
  @callback format_discarded(discarded :: non_neg_integer(), state :: term()) :: boolean()

  @optional_callbacks prepare_messages: 2,
                      handle_batch: 4,
                      handle_failed: 2,
                      process_name: 2,
                      format_discarded: 2

  defguardp is_broadway_name(name)
            when is_atom(name) or (is_tuple(name) and tuple_size(name) == 3)

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

  The Broadway options are:

  #{NimbleOptions.docs(Broadway.Options.definition())}

  """
  @spec start_link(module(), keyword()) :: on_start()
  def start_link(module, opts) do
    case NimbleOptions.validate(opts, Broadway.Options.definition()) do
      {:error, error} ->
        raise ArgumentError, format_error(error)

      {:ok, opts} ->
        Enum.each(opts[:batchers], fn {_batcher_name, batcher_opt} ->
          if batcher_opt[:max_demand] == nil and is_tuple(batcher_opt[:batch_size]) do
            raise ArgumentError,
                  "you must set the :max_demand option to an integer when :batch_size is a tuple"
          end
        end)

        opts =
          opts
          |> carry_over_one(:producer, [:hibernate_after, :spawn_opt])
          |> carry_over_many(:processors, [:partition_by, :hibernate_after, :spawn_opt])
          |> carry_over_many(:batchers, [:partition_by, :hibernate_after, :spawn_opt])

        Topology.start_link(module, opts)
    end
  end

  defp format_error(%ValidationError{keys_path: [], message: message}) do
    "invalid configuration given to Broadway.start_link/2, " <> message
  end

  defp format_error(%ValidationError{keys_path: keys_path, message: message}) do
    "invalid configuration given to Broadway.start_link/2 for key #{inspect(keys_path)}, " <>
      message
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
  @doc since: "0.5.0"
  @spec producer_names(name()) :: [name()]
  def producer_names(broadway) when is_broadway_name(broadway) do
    Topology.producer_names(broadway)
  end

  @doc """
  Returns the topology details for a pipeline.

  The stages that have the `:concurrency` field in their info indicate a list of
  processes running with that name prefix. Each process has `:name` as a
  prefix plus `_` and the index of `0..(concurrency - 1)` as an atom. For example, a
  producer named `MyBroadway.Broadway.Producer` with concurrency of `1`
  will only have a single process named `MyBroadway.Broadway.Producer_0` in its
  topology.

  > #### Single producer and processor {: .info}
  >
  > Broadway does not accept multiple producers neither
  > multiple processors, but we chose to keep in a list
  > for consistency and to ensure we're future proof.

  ## Examples

      iex> Broadway.topology(MyBroadway)
      [
        producers: [
          %{name: MyBroadway.Broadway.Producer, concurrency: 1}
        ],
        processors: [
          %{name: MyBroadway.Broadway.Processor_default, concurrency: 10, processor_key: :default}
        ],
        batchers: [
          %{
            batcher_name: MyBroadway.Broadway.Batcher_default,
            name: MyBroadway.Broadway.BatchProcessor_default,
            batcher_key: :default,
            concurrency: 5
          },
          %{
            batcher_name: MyBroadway.Broadway.Batcher_s3,
            name: MyBroadway.Broadway.BatchProcessor_s3,
            batcher_key: :s3,
            concurrency: 3
          }
        ]
      ]

  In the example above, for instance, the processor process names would be
  `MyBroadway.Broadway.Processor_default_0`, `MyBroadway.Broadway.Processor_default_1`,
  and so on.
  """
  @doc since: "1.0.0"
  @spec topology(broadway :: name()) :: [{key, [stage_info]}]
        when key: :producers | :processors | :batchers,
             stage_info: %{
               required(:name) => atom(),
               optional(:concurrency) => pos_integer(),
               optional(:batcher_name) => atom(),
               optional(:batcher_key) => atom(),
               optional(:processor_key) => atom()
             }
  def topology(broadway) when is_broadway_name(broadway) do
    Topology.topology(broadway)
  end

  @doc """
  Returns all running Broadway names.

  It's important to notice that no order is guaranteed.
  """
  @doc since: "1.0.0"
  @spec all_running() :: [name()]
  def all_running do
    config_storage = ConfigStorage.get_module()

    for name <- config_storage.list(),
        (try do
           GenServer.whereis(name)
         rescue
           _ -> false
         end),
        do: name
  end

  @doc """
  Sends a list of `Broadway.Message`s to the Broadway pipeline.

  The producer is randomly chosen among all sets of producers/stages.
  This is used to send out of band data to a Broadway pipeline.
  """
  @spec push_messages(broadway :: name(), messages :: [Message.t()]) :: :ok
  def push_messages(broadway, messages) when is_broadway_name(broadway) and is_list(messages) do
    broadway
    |> producer_names()
    |> Enum.random()
    |> Topology.ProducerStage.push_messages(messages)
  end

  test_batch_options_schema = [
    metadata: [
      type: :any,
      default: [],
      doc: """
      an enumerable of key-value pairs of *additional* fields to add to the
      message. This can be used, for example, when testing `BroadwayRabbitMQ.Producer`.
      """
    ],
    acknowledger: [
      type: {:fun, 2},
      doc: """
      a function that generates `ack` fields of the sent `Broadway.Message.t()`.
      This function receives the acknowledger `data` and the `from` field and
      it must return the acknowledger tuple. The typespec of this function is:

      `data :: term(), from :: {pid(), term()} -> {module(), ack_ref :: term(), ack_data :: term()}`
      """
    ],
    batch_mode: [
      type: {:in, [:bulk, :flush]},
      default: :bulk,
      doc: """
      when set to `:flush`, the batch the message is in is immediately delivered. When set
      to `:bulk`, batch is delivered when its size or timeout is reached.
      """
    ]
  ]

  @test_batch_options_schema NimbleOptions.new!(test_batch_options_schema)

  @test_message_options_schema test_batch_options_schema
                               |> Keyword.delete(:batch_mode)
                               |> NimbleOptions.new!()

  @doc """
  Sends a test message through the Broadway pipeline.

  This is a convenience used for testing. The given data
  is automatically wrapped in a `Broadway.Message` with
  `Broadway.CallerAcknowledger` configured to send a message
  back to the caller once the message has been fully processed.

  The message is set to be flushed immediately, without waiting
  for the Broadway pipeline `batch_size` to be filled or the
  `batch_timeout` to be triggered.

  It returns a reference that can be used to identify the ack
  messages.

  See ["Testing"](#module-testing) section in module documentation
  for more information.

  ## Options

  #{NimbleOptions.docs(@test_message_options_schema)}

  ## Examples

  For example, in your tests, you may do:

      ref = Broadway.test_message(broadway, 1)
      assert_receive {:ack, ^ref, [successful], []}

  or if you want to override which acknowledger shall be called, you may do:

      acknowledger = fn data, ack_ref -> {MyAck, ack_ref, :ok} end
      Broadway.test_message(broadway, 1, acknowledger: acknowledger)

  Note that messages sent using this function will ignore demand and :transform
  option specified in :producer option in `Broadway.start_link/2`.
  """
  @spec test_message(broadway :: name(), term, opts :: Keyword.t()) :: reference
  def test_message(broadway, data, opts \\ [])
      when is_broadway_name(broadway) and is_list(opts) do
    opts = NimbleOptions.validate!(opts, @test_message_options_schema)

    test_messages(broadway, [data], _batch_mode = :flush, opts)
  end

  @doc """
  Sends a list of data as a batch of messages to the Broadway pipeline.

  This is a convenience used for testing. Each message is automatically
  wrapped in a `Broadway.Message` with `Broadway.CallerAcknowledger`
  configured to send a message back to the caller once all batches
  have been fully processed.

  If there are more messages in the batch than the pipeline `batch_size`
  or if the messages in the batch take more time to process than
  `batch_timeout` then the caller will receive multiple messages.

  It returns a reference that can be used to identify the ack
  messages.

  See ["Testing"](#module-testing) section in module documentation
  for more information.

  ## Options

  #{NimbleOptions.docs(@test_batch_options_schema)}

  ## Examples

  For example, in your tests, you may do:

      ref = Broadway.test_batch(broadway, [1, 2, 3])
      assert_receive {:ack, ^ref, successful, failed}, 1000
      assert length(successful) == 3
      assert length(failed) == 0

  Note that messages sent using this function will ignore demand and :transform
  option specified in :producer option in `Broadway.start_link/2`.
  """
  @spec test_batch(broadway :: name(), data :: [term], opts :: Keyword.t()) :: reference
  def test_batch(broadway, batch_data, opts \\ [])
      when is_broadway_name(broadway) and is_list(batch_data) and is_list(opts) do
    opts = NimbleOptions.validate!(opts, @test_batch_options_schema)
    {batch_mode, opts} = Keyword.pop(opts, :batch_mode, :bulk)
    test_messages(broadway, batch_data, batch_mode, opts)
  end

  defp test_messages(broadway, data, batch_mode, opts) when is_broadway_name(broadway) do
    metadata = opts |> Keyword.fetch!(:metadata) |> Map.new()

    acknowledger =
      Keyword.get(opts, :acknowledger, fn _data, ack_ref ->
        Broadway.CallerAcknowledger.init(ack_ref, :ok)
      end)

    ref = make_ref()

    messages =
      Enum.map(data, fn data ->
        ack = acknowledger.(data, {self(), ref})
        %Message{data: data, acknowledger: ack, batch_mode: batch_mode, metadata: metadata}
      end)

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
  @spec get_rate_limiting(server :: name()) ::
          {:ok, rate_limiting_info} | {:error, :rate_limiting_not_enabled}
        when rate_limiting_info: %{
               required(:interval) => non_neg_integer(),
               required(:allowed_messages) => non_neg_integer()
             }
  def get_rate_limiting(broadway) when is_broadway_name(broadway) do
    with {:ok, rate_limiter_name} <- Topology.get_rate_limiter(broadway) do
      {:ok, Topology.RateLimiter.get_rate_limiting(rate_limiter_name)}
    end
  end

  @update_rate_limiting_options_schema NimbleOptions.new!(
                                         allowed_messages: [type: :pos_integer],
                                         interval: [type: :pos_integer],
                                         reset: [type: :boolean]
                                       )

  @doc """
  Updates the producer rate limiting of the given pipeline at runtime.

  Supports the following options (see the `:rate_limiting` options in the module
  documentation for more information):

    * `:allowed_messages`
    * `:interval`
    * `:reset`

  Returns an `{:error, reason}` tuple if the given `broadway` pipeline doesn't
  have rate limiting enabled.

  The option `:reset` defaults to `false`. This means the rate limit will reset
  to the new rate limit at the end of the current interval. When `:reset` is `true`,
  the new rate limit takes effect immediately.

  ## Examples

      Broadway.update_rate_limiting(broadway, allowed_messages: 100)

  """
  @doc since: "0.6.0"
  @spec update_rate_limiting(server :: name(), opts :: Keyword.t()) ::
          :ok | {:error, :rate_limiting_not_enabled}
  def update_rate_limiting(broadway, opts) when is_broadway_name(broadway) and is_list(opts) do
    with {:validate_opts, {:ok, opts}} <-
           {:validate_opts, NimbleOptions.validate(opts, @update_rate_limiting_options_schema)},
         {:get_name, {:ok, rate_limiter_name}} <-
           {:get_name, Topology.get_rate_limiter(broadway)} do
      Topology.RateLimiter.update_rate_limiting(rate_limiter_name, opts)
    else
      {:validate_opts, {:error, %ValidationError{message: message}}} ->
        raise ArgumentError, "invalid options, " <> message

      {:get_name, {:error, reason}} ->
        {:error, reason}
    end
  end

  @doc """
  Synchronously stops the Broadway pipeline with the given `reason`.

  This function returns `:ok` if the pipeline terminates with the
  given reason; if it terminates with another reason, the call exits.

  This function keeps OTP semantics regarding error reporting.
  If the reason is any other than `:normal`, `:shutdown` or
  `{:shutdown, _}`, an error report is logged.
  """
  @doc since: "1.0.0"
  def stop(broadway, reason \\ :normal, timeout \\ :infinity)
      when is_broadway_name(broadway) or is_pid(broadway) do
    GenServer.stop(broadway, reason, timeout)
  end
end
