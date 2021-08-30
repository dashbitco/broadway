# RabbitMQ

RabbitMQ is an open source message broker designed to be highly scalable and
distributed. It supports multiple protocols including the Advanced Message
Queuing Protocol (AMQP).

## Getting Started

In order to use Broadway with RabbitMQ, we need to:

  1. [Create a queue](#create-a-queue) (or use an existing one)
  1. [Configure our Elixir project to use Broadway](#configure-the-project)
  1. [Define your pipeline configuration](#define-the-pipeline-configuration)
  1. [Implement Broadway callbacks](#implement-broadway-callbacks)
  1. [Run the Broadway pipeline](#run-the-broadway-pipeline)
  1. [Tuning the configuration](#tuning-the-configuration) (Optional)

In case you want to work with an existing queue, you can skip [step 1](#create-a-queue)
and jump to [Configure the project](#configure-the-project).

> Note: `BroadwayRabbitMQ` does not automatically create any queue. If you
configure a pipeline with a non-existent queue, the producers will crash,
bringing down the pipeline.

## Create a queue

RabbitMQ runs on many operating systems. Please see
[Downloading and Installing RabbitMQ](https://www.rabbitmq.com/download.html) for
further information. Also, make sure you have the
[Management](https://www.rabbitmq.com/management.html) plugin enabled, which ships
with the command line tool, `rabbitmqadmin`.

After successfully installing RabbitMQ, you can declare a new queue with the
following command:

    $ rabbitmqadmin declare queue name=my_queue durable=true

You can list all declared queues to see our the one we've just created:

    $ rabbitmqctl list_queues
    Timeout: 60.0 seconds ...
    Listing queues for vhost / ...
    name      messages
    my_queue  0

## Configure the project

In this guide, we're going to use [BroadwayRabbitMQ](https://github.com/dashbitco/broadway_rabbitmq),
which is a Broadway RabbitMQ Connector provided by [Dashbit](https://dashbit.co/).

### Starting a new project

If you're creating a new project, run:

    $ mix new my_app --sup

The `--sup` flag instructs Elixir to generate an application with a supervision tree.

### Setting up dependencies

Add `:broadway_rabbitmq` to the list of dependencies in `mix.exs`:

    def deps do
      [
        ...
        {:broadway_rabbitmq, "~> 0.7"},
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
          name: MyBroadway,
          producer: [
            module: {BroadwayRabbitMQ.Producer,
              queue: "my_queue",
              qos: [
                prefetch_count: 50,
              ]
            },
            concurrency: 1
          ],
          processors: [
            default: [
              concurrency: 50
            ]
          ],
          batchers: [
            default: [
              batch_size: 10,
              batch_timeout: 1500,
              concurrency: 5
            ]
          ]
        )
      end

      ...callbacks...
    end

If you're consuming data from an existing broker that requires authorization,
you'll need to provide your credentials using the `connection` option:

    ...
    producer: [
      module: {BroadwayRabbitMQ.Producer,
        queue: "my_queue",
        connection: [
          username: "user",
          password: "password",
        ]
        ...
      }
    ]
    ...

For the full list of `connection` options, please see
[`AMQP.Connection.open/1`](https://hexdocs.pm/amqp/1.1.1/AMQP.Connection.html#open/1)

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
> the `:batchers` configuration along with the `handle_batch/4` callback. This
> is perfectly fine for RabbitMQ, where messages are acknowledged individually
> and never as a batch.

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

    $ iex -S mix

If everything went fine, you should see lots of `info` log messages from the `amqp`
supervisors. If you think that's too verbose and want to do something
about it, please take a look at the _"Log related to amqp supervisors are too verbose"_
subsection in the `amqp`'s  [Troubleshooting](https://hexdocs.pm/amqp/readme.html#troubleshooting)
documentation.

Finally, let's generate some sample messages to be consumed by Broadway with the
following code:

    {:ok, connection} = AMQP.Connection.open
    {:ok, channel} = AMQP.Channel.open(connection)
    AMQP.Queue.declare(channel, "my_queue", durable: true)

    Enum.each(1..5000, fn i ->
      AMQP.Basic.publish(channel, "", "my_queue", "#{i}")
    end)
    AMQP.Connection.close(connection)

You should see the output showing the generated batches:

    Got batch: [
      {"7", 14},
      {"5", 10},
      {"8", 16},
      {"98", 196},
      {"6", 12},
      {"97", 194},
      {"9", 18},
      {"99", 198},
      {"10", 20},
      {"100", 200}
    ]
    Got batch: [
      {"29", 58},
      {"32", 64},
      ...
    ]

## Tuning the configuration

Some of the configuration options available for Broadway come already with a
"reasonable" default value. However, those values might not suit your
requirements. Depending on the number of messages you get, how much processing
they need and how much IO work is going to take place, you might need completely
different values to optimize the flow of your pipeline. The `concurrency` option
available for every set of producers, processors and batchers, among with
`max_demand`, `batch_size`, and `batch_timeout` can give you a great deal
of flexibility. The `concurrency` option controls the concurrency level in
each layer of the pipeline.
See the notes on [`Producer concurrency`](https://hexdocs.pm/broadway/Broadway.html#module-producer-concurrency)
and [`Batcher concurrency`](https://hexdocs.pm/broadway/Broadway.html#module-batcher-concurrency)
for details.

Another important option to take into account is the `:prefetch_count`.
RabbitMQ will continually push new messages to Broadway as it receives them.
The `:prefetch_count` setting provides back-pressure by instructing RabbitMQ to [limit the number of unacknowledged messages a consumer will have at a given moment](https://www.rabbitmq.com/consumer-prefetch.html).
See the ["Back-pressure and :prefetch_count"](https://hexdocs.pm/broadway_rabbitmq/BroadwayRabbitMQ.Producer.html#module-back-pressure-and-prefetch_count)
section of the `BroadwayRabbitMQ` documentation for details.

In order to get a good set of configurations for your pipeline, it's
important to respect the limitations of the servers you're running,
as well as the limitations of the services you're providing/consuming
data to/from. Broadway comes with telemetry, so you can measure your
pipeline and help ensure your changes are effective.
