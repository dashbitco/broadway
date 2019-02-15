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
  1. Implement Broadway's callbacks so we can process incoming messages
  1. Add Broadway to the application supervision tree (Optional)
  1. Tuning the configuration (Optional)

## Create a SQS queue

Amazon provides a comprehensive [Step-by-step Guide](https://aws.amazon.com/getting-started/tutorials/send-messages-distributed-applications/)
on creating SQS queues. In case you don't have an AWS account and want to
test Broadway locally, use can easily install [ElasticMQ](https://github.com/softwaremill/elasticmq),
which is a message queue system that offers a SQS-compatible query interface.

## Configure the project

In this guide we're going to use the
[BroadwaySQS](https://github.com/plataformatec/broadway_sqs), which is a
Broadway SQS Connector provided by Plataformatec.

### Setting up dependencies

Add `:broadway_sqs` to the list of dependencies in `mix.exs` along the HTTP
client of your choice (defaults to `:hackney`):

```elixir
def deps do
  [
    ...
    {:broadway_sqs, "~> 0.1.0"},
    {:hackney, "~> 1.9"},
  ]
end
```

## Define the pipeline configuration

Like any other process-based behaviour, implementing the Broadway
bahaviour is straightforward. The second argument of
`Broadway.start_link/2` is the pipeline configuration. Assuming we
want to consume messages from our queue called `my_queue`. The minimal
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
and `AWS_SECRET_ACCESS_KEY` environment variables configured. If that's
not the case, you will need to pass that information to the client so it
can properly connect to the the AWS servers. Here is how you can do it:


    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        default: [
          module: BroadwaySQS.SQSProducer,
          arg: [
            sqs_client: {BroadwaySQS.ExAwsClient, [
              queue_name: "my_queue",
              config: [
                access_key_id: "YOUR_AWS_ACCESS_KEY_ID",
                secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY"
              ]
            ]}
          ]
        ]
      ],
      ...
    )

For a full list of config options, please see [ExAws](https://hexdocs.pm/ex_aws/)
documentation.

## Implement Broadway's callbacks

In order to process incoming messages, we need to implement the
required callbacks. For the sake of simplicity, we're considering that
all messages received from the queue are numbers:

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

We are not doing anything fancy here. First we update the message's data
individually inside `handle_message/3` and then we print each batch inside
handle_batch/4.

## Add Broadway to the application supervision tree (Optional)
TODO

## Tuning the configuration
TODO
