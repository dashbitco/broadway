defmodule Broadway.DummyProducerTest do
  use ExUnit.Case, async: true

  defmodule Handler do
    def handle_message(_processor, message, _context) do
      message
    end
  end

  test "send message through", c do
    {:ok, _} =
      Broadway.start_link(Handler,
        name: c.test,
        producer: [
          module: {Broadway.DummyProducer, []}
        ],
        processors: [
          default: [
            concurrency: 1
          ]
        ]
      )

    ref = Broadway.test_messages(c.test, [1, 2])
    assert_receive {:ack, ^ref, [%{status: :ok}, %{status: :ok}], []}
  end
end
