# Apache Kafka

Kafka is a distributed streaming platform that has three key capabilities:

  * Publish and subscribe to streams of records
  * Store streams of records in a fault-tolerant durable way
  * Process streams of records as they occur

## Getting Started

In order to use Broadway with Kafka, we need to:

  1. Create a stream of records (or use an existing one)
  1. Configure your Elixir project to use Broadway
  1. Define your pipeline configuration
  1. Implement Broadway callbacks
  1. Run the Broadway pipeline

## Create a stream of records (or use an existing one)

In case you don't have Kafka installed yet, please follow the instructions on Kafka's
[Quickstart](https://kafka.apache.org/quickstart) for a clean installation. After
initializing Kafka, you can create a new stream by running:

    kafka-topics --create --zookeeper localhost:2181 --partitions 3 --topic test

## Configure your Elixir project to use Broadway

This guide describes the steps necessary to integrate Broadway with Kafka using
[BroadwayKafka](https://github.com/plataformatec/broadway_kafka),
which is a Broadway Kafka Connector provided by [Plataformatec](http://www.plataformatec.com).

BroadwayKafka can subscribe to one or more topics and process streams of records
using Kafka's [Consumer API](https://kafka.apache.org/documentation.html#consumerapi).

Each GenStage producer initialized by BroadwayKafka will be available as a consumer,
all registered using the same self-labeled **consumer group**. Each record published to a
topic/partition will be delivered to one consumer instance within each consumer group.

Bear in mind that a topic/partition can be assigned to any consumer instance that has
been subscribed using the same consumer group, i.e, any Broadway instance or application
running on any machine connected to the Kafka cluster.

### Starting a new project

Create a new project running:

    mix new my_app --sup

The `--sup` flag instructs Elixir to generate an application with a supervision tree.

### Setting up dependencies

Add `:broadway_kafka` to the list of dependencies in `mix.exs`:

    def deps do
      [
        ...
        {:broadway_kafka, "~> 0.1.0"}
      ]
    end

Don't forget to check for the latest version of dependencies.

## Define the pipeline configuration

Broadway is a process-based behaviour and to define a Broadway pipeline,
we need to define three functions: `start_link/1`, `handle_message/3`
and optionally `handle_batch/4`. We will cover `start_link/1` in this
section and the `handle_` callbacks in the next one.

Similar to other process-based behaviours, `start_link/1` simply
delegates to `Broadway.start_link/2`, which should define the
producers, processors, and batchers in the Broadway pipeline.
Assuming we want to consume messages from a queue called
`my_queue`, one possible configuration would be:

    defmodule MyBroadway do
      use Broadway

      alias Broadway.Message

      def start_link(_opts) do
        Broadway.start_link(__MODULE__,
          name: __MODULE__,
          producer: [
            module: {BroadwayKafka.Producer, [
              hosts: [localhost: 9092],
              group_id: "group_1",
              topics: ["test"],
            ]},
            stages: 10
          ],
          processors: [
            default: [
              stages: 10
            ]
          ],
          batchers: [
            default: [
              batch_size: 100,
              batch_timeout: 200,
              stages: 10
            ]
          ]
        )
      end

      ...callbacks...
    end

> **Note**: Pipelines built on top of BroadwayKafka are automatically partitioned.
So even though there are multiple processes (stages), these processes will preserve
Kafka's ordering semantics when it comes to topics/partitions. Internally, this is
achieved by making sure all messages from the same topic/partition will always be
forwarded to the same processor and batch processor.

For a full list of options for `BroadwayKafka.Producer`, refer to the
official [BroadwayKafka](https://hexdocs.pm/broadway_kafka/) documentation.

For general information about setting up Broadway, see `Broadway`
module docs as well as `Broadway.start_link/2`.

## Implement Broadway callbacks

In order to process incoming messages, we need to implement the
required callbacks. For the sake of simplicity, we're considering that
all messages received from the queue are just numbers:

    defmodule MyBroadway do
      use Broadway

      alias Broadway.Message

      ...start_link...

      @impl true
      def handle_message(_, message, _) do
        message
        |> Message.update_data(fn data -> {data, String.to_integer(data) * 2} end)
      end

      @impl true
      def handle_batch(_, messages, _, _) do
        list = messages |> Enum.map(fn e -> e.data end)
        IO.inspect(list, label: "Got batch")
        messages
      end
    end

We are not doing anything fancy here, but it should be enough for our
purpose. First, we update the message's data individually inside
`handle_message/3` and then we print each batch inside `handle_batch/4`.

For more information, see `c:Broadway.handle_message/3` and
`c:Broadway.handle_batch/4`.

> Note: Since Broadway v0.2, batching is optional. In case you don't need to
> group messages as batches for further processing/publishing, you can remove
> the `:batchers` configuration along with the `handle_batch/4` callback.

## Run the Broadway pipeline

To run your `Broadway` pipeline, you just need to add as a child in
a supervision tree. Most applications have a supervision tree defined
at `lib/my_app/application.ex`. You can add Broadway as a child to a
supervisor as follows:

    children = [
      {MyBroadway, []}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)

Now the Broadway pipeline should be started when your application starts.
Also, if your Broadway has any dependency (for example, it needs to talk
to the database), make sure that Broadway is listed *after* its dependencies
in the supervision tree.

You can now test your pipeline by entering an `iex` session:

    iex -S mix

If everything went fine, you should see lots of `info` log messages like this
one coming from the `:brod` supervisors:

    15:14:04.356 [info]  [supervisor: {:local, :brod_sup}, started: [pid: #PID<0.251.0>, id: :test_client, mfargs: {:brod_client, :start_link, [[localhost: 9092], :test_client, []]}, restart_type: {:permanent, 10}, shutdown: 5000, child_type: :worker]]

 [Brod](https://github.com/klarna/brod/) is the client that BroadwayKafka uses
 under the hood to communicate with Kafka.

### Sending messages to Kafka

Finally, we can send some sample messages to Kafka using using `:brod` with the following snippet:

    topic = "test"
    client_id = :my_client
    hosts = [localhost: 9092]

    :ok = :brod.start_client(hosts, client_id, _client_config=[])
    :ok = :brod.start_producer(client_id, topic, _producer_config = [])

    Enum.each(1..1000, fn i ->
      partition = rem(i, 3)
      :ok = :brod.produce_sync(client_id, topic, partition, _key="", "#{i}")
    end)

You should see the output showing the generated batches:

    Got batch: [
      {"2", 4},
      {"5", 10},
      {"8", 16},
      {"11", 22},
      {"14", 28},
      ...
    ]
    Got batch: [
      {"3", 6},
      {"6", 12},
      {"9", 18},
      {"12", 24},
      {"15", 30},
      ...
    ]
