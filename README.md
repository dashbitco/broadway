# Broadway

Broadway makes it straight-forward to build concurrent and multi-stage data ingestion and data processing pipelines with Elixir.

Documentation can be found at [https://hexdocs.pm/broadway](https://hexdocs.pm/broadway).

## Built-in features

  * Back-pressure
  * Batching
  * Fault tolerance through restarts
  * Clean shutdown
  * Rate-limiting (TODO)
  * Partitioning (TODO)
  * Statistics/Metrics (TODO)
  * Back-off (TODO)

## Installation

Add `:broadway` to the list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:broadway, "~> 0.1.0"}
  ]
end
```

## Example

```elixir
defmodule MyBroadway do
  use Broadway

  alias Broadway.Message
  alias BroadwaySQS.{SQSProducer, ExAwsClient}

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        sqs: [
          module: SQSProducer,
          arg: [sqs_client: {ExAwsClient, [queue_name: "my_queue"]}]
        ]
      ],
      processors: [
        default: [stages: 50]
      ],
      batchers: [
        s3: [stages: 5, batch_size: 10, batch_timeout: 1000]
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
    # Do some calculations, generate a JSON representation, etc.
  end
end
```

## License

Copyright 2019 Plataformatec

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
