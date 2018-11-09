defmodule BroadwayTest do
  use ExUnit.Case

  import Integer
  alias Broadway.Message

  defmodule Counter do
    use GenStage

    @behaviour Broadway.Acknowledger

    def start_link(args, opts \\ []) do
      GenStage.start_link(__MODULE__, args, opts)
    end

    def init(args) do
      from = Keyword.fetch!(args, :from)
      to = Keyword.fetch!(args, :to)
      {:producer, %{to: to, counter: from}}
    end

    def handle_demand(demand, %{to: to, counter: counter} = state)
        when demand > 0 and counter > to do
      {:noreply, [], state}
    end

    def handle_demand(demand, %{to: to, counter: counter} = state) when demand > 0 do
      max = min(to, counter + demand - 1)

      events =
        Enum.map(counter..max, fn e ->
          %Message{data: e, acknowledger: {__MODULE__, %{id: e}}}
        end)

      {:noreply, events, %{state | counter: max + 1}}
    end

    def ack(successful, failed, target_pid) do
      send(target_pid, {:ack, successful, failed})
    end
  end

  defmodule Forwarder do
    @behaviour Broadway

    import Message.Actions

    def handle_message(%Message{data: data} = message, target_pid) when is_odd(data) do
      send(target_pid, {:message_handled, message.data})

      {:ok,
       message
       |> update_data(fn data -> data + 1000 end)
       |> put_publisher(:odd)}
    end

    def handle_message(message, target_pid) do
      send(target_pid, {:message_handled, message.data})

      {:ok,
       message
       |> update_data(fn data -> data + 1000 end)
       |> put_publisher(:even)}
    end

    def handle_batch(publisher, batch, target_pid) do
      send(target_pid, {:batch_handled, publisher, batch.events})
      {:ack, successful: batch.events, failed: []}
    end
  end

  defmodule ForwarderWithNoPublisherDefined do
    @behaviour Broadway

    def handle_message(message, _target_pid) do
      {:ok, message}
    end

    def handle_batch(publisher, batch, target_pid) do
      send(target_pid, {:batch_handled, publisher, batch.events})
      {:ack, successful: batch.events, failed: []}
    end
  end

  describe "processor" do
    test "handle all produced messages" do
      {:ok, _} =
        Broadway.start_link(Forwarder, self(),
          producers: [[module: Counter, arg: [from: 1, to: 200]]],
          publishers: [:even, :odd]
        )

      for counter <- 1..200 do
        assert_receive {:message_handled, ^counter}
      end

      refute_receive {:message_handled, _}
    end

    test "forward messages to the specified batcher" do
      {:ok, _} =
        Broadway.start_link(Forwarder, self(),
          producers: [[module: Counter, arg: [from: 1, to: 200]]],
          publishers: [:even, :odd]
        )

      assert_receive {:batch_handled, :odd, messages}
      assert Enum.all?(messages, fn msg -> is_odd(msg.data) end)

      assert_receive {:batch_handled, :even, messages}
      assert Enum.all?(messages, fn msg -> is_even(msg.data) end)

      refute_receive {:batch_handled, _, _}
    end
  end

  describe "publisher/batcher" do
    test "generate batches based on :batch_size" do
      {:ok, _} =
        Broadway.start_link(Forwarder, self(),
          producers: [[module: Counter, arg: [from: 1, to: 40]]],
          publishers: [
            odd: [batch_size: 10],
            even: [batch_size: 5]
          ]
        )

      assert_receive {:batch_handled, :odd, messages} when length(messages) == 10
      assert_receive {:batch_handled, :odd, messages} when length(messages) == 10
      assert_receive {:batch_handled, :even, messages} when length(messages) == 5
      assert_receive {:batch_handled, :even, messages} when length(messages) == 5
      assert_receive {:batch_handled, :even, messages} when length(messages) == 5
      assert_receive {:batch_handled, :even, messages} when length(messages) == 5
      refute_receive {:batch_handled, _, _}
    end

    test "generate batches with the remaining messages when :batch_timeout is reached" do
      {:ok, _} =
        Broadway.start_link(Forwarder, self(),
          producers: [[module: Counter, arg: [from: 1, to: 5]]],
          publishers: [
            odd: [batch_size: 10, batch_timeout: 20],
            even: [batch_size: 10, batch_timeout: 20]
          ]
        )

      assert_receive {:batch_handled, :odd, messages} when length(messages) == 3
      assert_receive {:batch_handled, :even, messages} when length(messages) == 2
      refute_receive {:batch_handled, _, _}
    end
  end

  describe "publisher/consumer" do
    test "handle all generated batches" do
      {:ok, _} =
        Broadway.start_link(Forwarder, self(),
          producers: [[module: Counter, arg: [from: 1, to: 200]]],
          publishers: [
            odd: [batch_size: 10],
            even: [batch_size: 10]
          ]
        )

      for _ <- 1..20 do
        assert_receive {:batch_handled, _, _}
      end

      refute_receive {:batch_handled, _, _}
    end

    test "pass messages to be acknowledged" do
      {:ok, _} =
        Broadway.start_link(Forwarder, self(),
          producers: [[module: Counter, arg: [from: 1, to: 200]]],
          publishers: [:odd, :even]
        )

      assert_receive {:ack, successful, _failed}
      assert length(successful) == 100
    end

    test "messages including extra data for acknowledgement" do
      {:ok, _} =
        Broadway.start_link(Forwarder, self(),
          producers: [[module: Counter, arg: [from: 1, to: 200]]],
          publishers: [:odd, :even]
        )

      assert_receive {:ack, successful, _failed}

      assert Enum.all?(successful, fn %Message{acknowledger: {_, ack_data}, data: data} ->
               data == ack_data.id + 1000
             end)
    end

    test "define a default publisher when none is defined" do
      {:ok, _} =
        Broadway.start_link(ForwarderWithNoPublisherDefined, self(),
          producers: [[module: Counter, arg: [from: 1, to: 100]]]
        )

      assert_receive {:batch_handled, :default, _messages}
    end

    test "default number of processors is schedulers_online * 2" do
      {:ok, broadway} = Broadway.start_link(Forwarder, self(), producers: [])
      schedulers_online = :erlang.system_info(:schedulers_online)

      assert length(:sys.get_state(broadway).processors) == schedulers_online * 2
    end

    test "set number of processors" do
      {:ok, broadway1} =
        Broadway.start_link(Forwarder, self(),
          processors: [stages: 5],
          producers: []
        )

      {:ok, broadway2} =
        Broadway.start_link(Forwarder, self(),
          processors: [stages: 10],
          producers: []
        )

      assert length(:sys.get_state(broadway1).processors) == 5
      assert length(:sys.get_state(broadway2).processors) == 10
    end
  end
end
