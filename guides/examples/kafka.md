# Kafka

https://kafka.apache.org/


## Getting Started

In order to use Broadway with Kakka, we need to:

  1. Create a Kafka topic (or use an existing one)
  1. Configure our Elixir project to use Broadway
  1. Define your pipeline configuration
  1. Run the Broadway pipeline

## Create a topic

    docker-compose -p broadway -f guides/examples/docker-compose.yml up
    docker exec kafka kafka-topics.sh  \
    	--create  \
    	--zookeeper zookeeper:2181  \
    	--topic raw_test_topic  \
    	--partitions 1 \ 
    	--replication-factor 1 \
    	--if-not-exists \
    	--config message.timestamp.type=LogAppendTime \
    	--config compression.type=snappy
    
    docker exec -it kafka kafka-console-producer.sh \
    	--broker-list localhost:9092 \
    	--topic raw_test_topic
    
    Type in 1<Enter>2<Enter><Ctrl-D>

### Setting up dependencies

Add `:kafka_gen_stage` to the list of dependencies in `mix.exs`:

```elixir
  def deps do
    [
      ...,
      {:broadway, "~> 0.1.0"},
      {:kafka_gen_stage, "~> 1.0.1"}
    ]
  end
```

## Define the pipeline configuration

```elixir
defmodule MyBroadway do
  @moduledoc """
  Pipeline that copies messages from one Kafka topic to another.
  After restart the pipeline continues with the last acked
  offset + 1. Restarts might create duplicates in output
  topic.
  """

  use Broadway
  alias Broadway.Message
  alias KafkaGenStage.Consumer
  
  @pipeline_id __MODULE__
  
  ###################################################################
  # simple offset handling
  ###################################################################
  defp offset_file do
    "/tmp/#{@pipeline_id}.offset"
  end

  defp load_offset do
    case File.read(offset_file()) do
      {:error, :enoent} ->
        0

      {:ok, binary} ->
        {w, _} = Integer.parse(binary)
        w + 1
    end
  end

  defp store_offset(offset) do
    File.write!(offset_file(), ["#{offset}"])
  end

  ###################################################################
  # pipeline definition
  ###################################################################
  def start_link(_opts) do
    offset = load_offset()
    IO.inspect("Starting pipeline #{@pipeline_id} at #{offset}")

    options = [
      transformer_state: [],
      transformer: fn kafka_message, state ->
        {[
           %Message{
             data: kafka_message,
             acknowledger: {__MODULE__, :ack, kafka_message}
           }
         ], state}
      end,
      begin_offset: offset
    ]

    brod_client = String.to_atom("#{__MODULE__}_kafka")

    out_topic = "test_output_topic"

    {:ok, _pid} =
      :brod.start_link_client([{'localhost', 9092}], brod_client,
        reconnect_cool_down_seconds: 30,
        restart_delay_seconds: 30
      )

    :ok = :brod.start_producer(brod_client, out_topic, [])

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        default: [
          module: {Consumer, {brod_client, "raw_test_topic", options}},
          stages: 1
        ]
      ],
      processors: [
        default: [
          stages: 2,
          max_demand: 1000
        ]
      ],
      batchers: [
        kafka: [stages: 1, batch_size: 100]
      ],
      context: [
        out_topic,
        brod_client
      ]
    )
  end

  @impl true
  def handle_message(_, %Message{data: _data} = message, _context) do
    message
    |> Message.put_batcher(:kafka)
  end

  @impl true
  def handle_batch(:kafka, messages, _batch_info, [topic, brod_client]) do
    partition = 0
    part_fun = fn _, _, _, _ -> {:ok, partition} end

    :ok =
      :brod.produce_sync(brod_client, topic, part_fun, "", Enum.map(messages,
        fn(%{data: {_offset, timestamp, key, value}}) ->
          {timestamp, key, value}
        end))

    # ack last offset
    [
      Enum.at(messages, -1)
    ]
  end

  def ack(_a, [%Broadway.Message{data: {offset, _ts, _, _}}], _) do
    store_offset(offset)
  end

  def ack(_a, [], errors) do
    IO.inspect({:something_wrong, errors})
  end
end
```

## Run the pipeline

```elixir
  Supervisor.start_link([{MyBroadway, []}], strategy: :one_for_one)
```
