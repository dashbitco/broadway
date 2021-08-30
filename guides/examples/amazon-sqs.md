# Amazon SQS

Amazon Simple Queue Service (SQS) is a highly scalable distributed message
queuing service provided by Amazon.com. AWS SQS offers two types of message
queues:

  * Standard
    * Nearly unlimited throughput
    * Best-effort ordering
    * At-least-once delivery

  * FIFO
    * Limited number of transactions per second (TPS).
      See [Amazon SQS FIFO](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html)
      developer guide for more information on limits.
    * Order in which messages are sent/received is strictly preserved
    * Exactly-once delivery

Broadway can work seamlessly with both, Standard and FIFO queues.

## Getting Started

In order to use Broadway with SQS, we need to:

  1. Create a SQS queue (or use an existing one)
  1. Configure our Elixir project to use Broadway
  1. Define your pipeline configuration
  1. Implement Broadway callbacks
  1. Run the Broadway pipeline
  1. Tuning the configuration (Optional)

## Create a SQS queue

Amazon provides a comprehensive [Step-by-step Guide](https://aws.amazon.com/getting-started/tutorials/send-messages-distributed-applications/)
on creating SQS queues. In case you don't have an AWS account and want to
test Broadway locally, use can easily install [ElasticMQ](https://github.com/softwaremill/elasticmq),
which is a message queue system that offers a SQS-compatible query interface.

## Configure the project

In this guide we're going to use [BroadwaySQS](https://github.com/dashbitco/broadway_sqs),
which is a Broadway SQS Connector provided by [Dashbit](https://dashbit.co/).

### Starting a new project

If you plan to start a new project, just run:

    $ mix new my_app --sup

The `--sup` flag instructs Elixir to generate an application with a supervision tree.

### Setting up dependencies

Add `:broadway_sqs` to the list of dependencies in `mix.exs` along the HTTP
client of your choice (defaults to `:hackney`):

    def deps do
      [
        ...
        {:broadway_sqs, "~> 0.7"},
        {:hackney, "~> 1.9"},
      ]
    end

Don't forget to check for the latest version of dependencies.

## Define the pipeline configuration

Broadway is a process-based behaviour and to define a Broadway
pipeline, we need to define three functions: `start_link/1`,
`handle_message/3` and `handle_batch/4`. We will cover `start_link/1`
in this section and the `handle_` callbacks in the next one.

Similar to other process-based behaviour, `start_link/1` simply
delegates to `Broadway.start_link/2`, which should define the
producers, processors, and batchers in the Broadway pipeline.
Assuming we want to consume messages from a queue called
`my_queue`, the minimal configuration would be:

    defmodule MyBroadway do
      use Broadway

      alias Broadway.Message

      def start_link(_opts) do
        Broadway.start_link(__MODULE__,
          name: __MODULE__,
          producer: [
            module: {BroadwaySQS.Producer,
                     queue_url: "https://us-east-2.queue.amazonaws.com/100000000001/my_queue"}
          ],
          processors: [
            default: []
          ],
          batchers: [
            default: [
              batch_size: 10,
              batch_timeout: 2000
            ]
          ]
        )
      end

      ...callbacks...
    end

The above configuration also assumes that you have the AWS credentials
set up in your environment, for instance, by having the `AWS_ACCESS_KEY_ID`
and `AWS_SECRET_ACCESS_KEY` environment variables set. If that's
not the case, you will need to pass that information to the client so it
can properly connect to the AWS servers. Here is how you can do it:

    ...
    producer: [
      module:
        {BroadwaySQS.Producer,
         queue_url: "https://us-east-2.queue.amazonaws.com/100000000001/my_queue",
         config: [
           access_key_id: "YOUR_AWS_ACCESS_KEY_ID",
           secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY"
         ]}
    ]
    ...

For a full list of options for `BroadwaySQS.Producer`, please see
[BroadwaySQS](https://hexdocs.pm/broadway_sqs/) documentation.

For general information about setting up Broadway, see `Broadway`
module docs as well as `Broadway.start_link/2`.

> Note: Even though batching is optional since Broadway v0.2, we recommend that all Amazon SQS
> pipelines have at least a default batcher. This lets you control the exact batch
> size and frequency that messages are acknowledged to Amazon SQS, often leading to
> pipelines that are more cost and time efficient.

## Implement Broadway callbacks

In order to process incoming messages, we need to implement the
required callbacks. For the sake of simplicity, we're considering that
all messages received from the queue are just numbers:

    defmodule MyBroadway do
      use Broadway

      alias Broadway.Message

      ...start_link...

      @impl true
      def handle_message(_, %Message{data: data} = message, _) do
        message
        |> Message.update_data(fn data -> data * data end)
      end

      @impl true
      def handle_batch(_, messages, _, _) do
        list = messages |> Enum.map(fn e -> e.data end)
        IO.inspect(list, label: "Got batch of finished jobs from processors, sending ACKs to SQS as a batch.")
        messages
      end
    end

We are not doing anything fancy here, but it should be enough for our
purpose. First we update the message's data individually inside
`handle_message/3` and then we print each batch inside `handle_batch/4`.

For more information, see `c:Broadway.handle_message/3` and
`c:Broadway.handle_batch/4`.

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

## Tuning the configuration

Some of the configuration options available for Broadway come already with a
"reasonable" default value. However those values might not suit your
requirements. Depending on the number of messages you get, how much processing
they need and how much IO work is going to take place, you might need completely
different values to optimize the flow of your pipeline. The `concurrency` option
available for every set of producers, processors and batchers, among with
`max_demand`, `batch_size`, and `batch_timeout` can give you a great deal
of flexibility.

The `concurrency` option controls the concurrency level in each layer of
the pipeline.
See the notes on [`Producer concurrency`](https://hexdocs.pm/broadway/Broadway.html#module-producer-concurrency)
and [`Batcher concurrency`](https://hexdocs.pm/broadway/Broadway.html#module-batcher-concurrency)
for details.

Here's an example on how you could tune them according to
your needs.

    defmodule MyBroadway do
      use Broadway

      def start_link(_opts) do
        Broadway.start_link(__MODULE__,
          name: __MODULE__,
          producer: [
            ...
            concurrency: 10,
          ],
          processors: [
            default: [
              concurrency: 100,
              max_demand: 1,
            ]
          ],
          batchers: [
            default: [
              batch_size: 10,
              concurrency: 10,
            ]
          ]
        )
      end

      ...callbacks...
    end

In order to get a good set of configurations for your pipeline, it's
important to respect the limitations of the servers you're running,
as well as the limitations of the services you're providing/consuming
data to/from. Broadway comes with telemetry, so you can measure your
pipeline and help ensure your changes are effective.
