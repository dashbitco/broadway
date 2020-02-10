# Broadway

Build concurrent and multi-stage data ingestion and data processing pipelines with Elixir. It allows developers to consume data efficiently from different sources, known as producers, such as Amazon SQS, Apache Kafka, Google Cloud PubSub, RabbitMQ, and others.

The name Broadway was taken from the famous [Broadway street](https://en.wikipedia.org/wiki/Broadway_theatre) in New York City, renowned by its stages and producers. :)

Documentation, examples, and how tos can be found at [https://hexdocs.pm/broadway](https://hexdocs.pm/broadway).

## Built-in features

Broadway takes the burden of defining concurrent GenStage topologies and provide a simple configuration API that automatically defines concurrent producers, concurrent processing, batch handling, and more, leading to both time and cost efficient ingestion and processing of data. It features:

  * Back-pressure
  * Automatic acknowledgements at the end of the pipeline
  * Batching
  * Fault tolerance with minimal data loss
  * Graceful shutdown
  * Built-in testing
  * Custom failure handling
  * Ordering and partitioning
  * Rate-limiting
  * Metrics
  * Back-off (TODO)

## Installation

Add `:broadway` to the list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:broadway, "~> 0.5.0"}
  ]
end
```

## Official Broadway Producers

Currently we officially support three Broadway producers:

  * Amazon SQS: [Source](https://github.com/dashbitco/broadway_sqs) - [Guide](https://hexdocs.pm/broadway/amazon-sqs.html)
  * Google Cloud Pub/Sub: [Source](https://github.com/dashbitco/broadway_cloud_pub_sub) - [Guide](https://hexdocs.pm/broadway/google-cloud-pubsub.html)
  * RabbitMQ: [Source](https://github.com/dashbitco/broadway_rabbitmq) - [Guide](https://hexdocs.pm/broadway/rabbitmq.html)

More producers are on the way.

### A quick example: SQS integration

Assuming you have added [`broadway_sqs`](https://github.com/dashbitco/broadway_sqs) as a dependency and configured your SQS credentials accordingly, you can consume Amazon SQS events in only 20 LOCs:

```elixir
defmodule MyBroadway do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {BroadwaySQS.Producer, queue_url: "https://us-east-2.queue.amazonaws.com/100000000001/my_queue"}
      ],
      processors: [
        default: [concurrency: 50]
      ],
      batchers: [
        s3: [concurrency: 5, batch_size: 10, batch_timeout: 1000]
      ]
    )
  end

  def handle_message(_processor_name, message, _context) do
    message
    |> Message.update_data(&process_data/1)
    |> Message.put_batcher(:s3)
  end

  def handle_batch(:s3, messages, _batch_info, _context) do
    # Send batch of messages to S3
  end

  defp process_data(data) do
    # Do some calculations, generate a JSON representation, process images.
  end
end
```

Once your Broadway module is defined, you just need to add it as a child of your application supervision tree as `{MyBroadway, []}`.

## Non-official (Off-Broadway) Producers

For those interested in rolling their own Broadway Producers (which we actively encourage!), we recommend using the `OffBroadway` namespace, mirroring the [Off-Broadway theaters](https://en.wikipedia.org/wiki/Off-Broadway). For example, if you want to publish your own integration with Amazon SQS, you can package it as `off_broadway_sqs`, which uses the `OffBroadway.SQS` namespace.

The following Off-Broadway libraries are available (feel free to send a PR adding your own in alphabetical order):

  * [off_broadway_kafka](https://github.com/bbalser/off_broadway_kafka): [Guide](https://hexdocs.pm/off_broadway_kafka/)
  * [off_broadway_redis](https://github.com/amokan/off_broadway_redis): [Guide](https://hexdocs.pm/off_broadway_redis/)

## Comparison to Flow

You may also be interested in [Flow by Dashbit](https://github.com/dashbitco/flow). Both Broadway and Flow are built on top of GenStage. Flow is a more general abstraction than Broadway that focuses on data as a whole, providing features like aggregation, joins, windows, etc. Broadway focuses on events and on operational features, such as metrics, automatic acknowledgements, failure handling, and so on.

## License

Copyright 2019 Plataformatec\
Copyright 2020 Dashbit

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
