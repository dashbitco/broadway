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
      See AWS SQS documentation for more information on limits.
    * Order in which messages are sent/received is strictly preserved 
    * Exactly-once delivery
  
Broadway can work seamlessly with both, Standard and FIFO queues.

## Getting Started

In order to use Broadway with SQS, we basically need to:

  1. Create a SQS queue (or use an existing one)
  1. Configure our Elixir project to use Broadway
  1. Define your pipeline configuration
  1. Implement Broadway callbacks, so we can process incoming messages
  1. Tuning the configuration (Optional)

## Create a SQS queue

Amazon provides a comprehensive [Step-by-step Guide](https://aws.amazon.com/getting-started/tutorials/send-messages-distributed-applications/)
on creating SQS queues. In case you don't have an AWS account and want to
test Broadway locally, use can easily install [ElasticMQ](https://github.com/softwaremill/elasticmq),
which is a message queue system that offers a SQS-compatible query interface.

## Configure the project

In this guide we're going to use [BroadwaySQS](https://github.com/plataformatec/broadway_sqs),
which is a Broadway SQS Connector provided by [Plataformatec](http://www.plataformatec.com).

### Setting up dependencies

Add `:broadway_sqs` to the list of dependencies in `mix.exs` along the HTTP
client of your choice (defaults to `:hackney`):

    def deps do
      [
        ...
        {:broadway_sqs, "~> 0.1.0"},
        {:hackney, "~> 1.9"},
      ]
    end

## Define the pipeline configuration

Like any other process-based behaviour, implementing the Broadway
bahaviour is straightforward. The second argument of
`Broadway.start_link/2` is the pipeline configuration. Assuming we
want to consume messages from a queue called `my_queue`. The minimal
configuration would be:

    defmodule MyBroadway do
      use Broadway
      alias BroadwaySQS.{SQSProducer, ExAwsClient}

      def start_link(_opts) do
        Broadway.start_link(__MODULE__,
          name: __MODULE__,
          producers: [
            default: [
              module: SQSProducer,
              arg: [
                sqs_client: {ExAwsClient, [
                  queue_name: "my_queue",
                ]}
              ]
            ]
          ],
          processors: [default: []],
          batchers: [default: []]
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
    sqs_client: {BroadwaySQS.ExAwsClient, [
      queue_name: "my_queue",
      config: [
        access_key_id: "YOUR_AWS_ACCESS_KEY_ID",
        secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY"
      ]
    ]}
    ...


For a full list of config options, please see [ExAws](https://hexdocs.pm/ex_aws/)
documentation. You can also find adicional options for both `BroadwaySQS.SQSProducer`
and `BroadwaySQS.ExAwsClient` in [BroadwaySQS](https://hexdocs.pm/broadway_sqs/) documentation.

## Implement Broadway callbacks

In order to process incoming messages, we need to implement the
required callbacks. For the sake of simplicity, we're considering that
all messages received from the queue are just numbers:

    defmodule MyBroadway do
      use Broadway
      alias BroadwaySQS.{SQSProducer, ExAwsClient}
      import Message

      ...start_link...
      
      def handle_message(_, %Message{data: data} = message, _) do
        message
        |> update_data(fn data -> data * data end)
      end

      def handle_batch(_, messages, _, _) do
        list = messages |> Enum.map(fn e -> e.data end)
        IO.inspect(list, label: "Got batch from SQS")
        messages
      end

    end

We are not doing anything fancy here, but it should be enough for our
purpose. First we update the message's data individually inside
`handle_message/3` and then we print each batch inside `handle_batch/4`.

## Tuning the configuration

Some of the configuration options available for Broadway come already with a
"reasonable" default value. However those values might not suit your
requirements. Depending on the number of messages you get, how much processing
they need and how much IO work is going to take place, you might need completely
different values to optimize the flow of your pipeline. The `stages` option
available for every set of producers, processors and batchers, among with
`batch_size` and `batch_timeout` can give you a great deal of flexibility.
The `stages` option controls the concurrency level in each layer of
the pipeline. Here's an example on how you could tune them according to
your needs.

    defmodule MyBroadway do
      use Broadway
      alias BroadwaySQS.{SQSProducer, ExAwsClient}

      def start_link(_opts) do
        Broadway.start_link(__MODULE__,
          name: __MODULE__,
          producers: [
            default: [
              ...
              stages: 60,
            ]
          ],
          processors: [
            default: [
              stages: 100,
            ]
          ],
          batchers: [
            default: [
              batch_size: 10,
              stages: 80,
            ]
          ]
        )
      end

      ...callbacks...
    end

In order to get a good set of configurations for your pipeline, it's
important to respect the limitations of the servers you're running,
as well as the limitations of the services you're providing/consuming
data to/from.