defmodule BroadwaySQSExample.IntSquared do
  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        default: [
          module:
            {BroadwaySQS.Producer,
             queue_name: Application.get_env(:broadway_sqs_example, :int_queue),
             config: [
               # access_key_id: "YOUR_AWS_ACCESS_KEY_ID",
               # secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY"
               region: "us-east-2"
             ]}
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

  def handle_message(_, %Message{data: data} = message, _) do
    IO.inspect(data)

    message
    |> Message.update_data(fn data -> String.to_integer(data) * String.to_integer(data) end)
  end

  def handle_batch(_, messages, _, _) do
    list = messages |> Enum.map(fn e -> e.data end)
    IO.inspect(list, label: "Got batch, sending acks to SQS")
    messages
  end
end
