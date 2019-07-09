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

In this guide we're going to use [BroadwaySQS](https://github.com/plataformatec/broadway_sqs),
which is a Broadway SQS Connector provided by [Plataformatec](http://www.plataformatec.com).

### Starting a new project

If you plan to start a new project, just run:

    mix new my_app --sup

The `--sup` flag instructs Elixir to generate an application with a supervision tree.

### Setting up dependencies

Add `:broadway_sqs` to the list of dependencies in `mix.exs` along the HTTP
client of your choice (defaults to `:hackney`):

    def deps do
      [
        ...
        {:broadway_sqs, "~> 0.2.0"},
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
          producers: [
            default: [
              module: {BroadwaySQS.Producer, queue_name: "my_queue"}
            ]
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
    producers: [
      default: [
        module: {BroadwaySQS.Producer,
          queue_name: "my_queue",
          config: [
            access_key_id: "YOUR_AWS_ACCESS_KEY_ID",
            secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY"
          ]
        }
      ]
    ]
    ...

For a full list of options for `BroadwaySQS.Producer`, please see
[BroadwaySQS](https://hexdocs.pm/broadway_sqs/) documentation.

For general information about setting up Broadway, see `Broadway`
module docs as well as `Broadway.start_link/2`.

> Note: Even though batching is optional in Broadway v0.2, we recommend
> all SQS pipelines to have at least a default batcher, with the default
> values defined above, unless you are expecting a very low rate of
> incoming messages. That's because batchers will also acknowledge
> messages in batches, which is the most cost and time efficient way
> of doing so on Amazon SQS.

## Implement Broadway callbacks

In order to process incoming messages, we need to implement the
required callbacks. For the sake of simplicity, we're considering that
all messages received from the queue are just numbers:

    defmodule MyBroadway do
      use Broadway

      alias Broadway.Message

      ...start_link...

      def handle_message(_, %Message{data: data} = message, _) do
        message
        |> Message.update_data(fn data -> data * data end)
      end

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

## Retrieving Metadata

By default the following information is added to the `metadata` field in the `%Message{}`
struct:

  * `message_id` - The message id received when the message was sent to the queue
  * `receipt_handle` - The receipt handle
  * `md5_of_body` - An MD5 digest of the message body

You can access any of that information directly while processing the message:

    def handle_message(_, message, _) do
      receipt = %{
        id: message.metadata.message_id,
        receipt_handle: message.metadata.receipt_handle
      }

      # Do something with the receipt
    end

If you want to retrieve `attributes` or `message_attributes`, you need to
configure the `:attributes_names` and `:message_attributes_names` options
accordingly, otherwise, attributes will not be attached to the response and
will not be available in the `metadata` field:

    ...

    producers: [
      default: [
        module: {BroadwaySQS.Producer,
          queue_name: "my_queue",
          # Define which attributes/message_attributes you want to be attached
          attribute_names: [:approximate_receive_count],
          message_attribute_names: ["SomeAttribute"],
        }
      ]
    ]

    ...

    def handle_message(_, message, _) do
      approximate_receive_count = message.metadata.attributes["approximate_receive_count"]
      some_attribute = message.metadata.message_attributes["SomeAttribute"]

      # Do something with the attributes
    end

For more information on the `:attributes_names` and `:message_attributes_names`
options, please see the [BroadwaySQS](https://hexdocs.pm/broadway_sqs/) documentation.

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
