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

    def ack(successful, failed, %{target_pid: target_pid}) do
      send(target_pid, {:ack, successful, failed})
    end
  end

  defmodule Forwarder do
    @behaviour Broadway

    import Message.Actions

    def handle_message(%Message{data: data} = message, %{target_pid: target_pid})
        when is_odd(data) do
      send(target_pid, {:message_handled, message.data})

      {:ok,
       message
       |> update_data(fn data -> data + 1000 end)
       |> put_publisher(:odd)}
    end

    def handle_message(message, %{target_pid: target_pid}) do
      send(target_pid, {:message_handled, message.data})

      {:ok,
       message
       |> update_data(fn data -> data + 1000 end)
       |> put_publisher(:even)}
    end

    def handle_batch(publisher, batch, %{target_pid: target_pid}) do
      send(target_pid, {:batch_handled, publisher, batch.messages})
      {:ack, successful: batch.messages, failed: []}
    end
  end

  defmodule ForwarderWithNoPublisherDefined do
    @behaviour Broadway

    def handle_message(message, _context) do
      {:ok, message}
    end

    def handle_batch(publisher, batch, %{target_pid: target_pid}) do
      send(target_pid, {:batch_handled, publisher, batch.messages})
      {:ack, successful: batch.messages, failed: []}
    end
  end

  defmodule ForwarderWithCustomHandlers do
    @behaviour Broadway

    def handle_message(message, %{handle_message: handler} = context) do
      handler.(message, context)
    end

    def handle_message(message, %{target_pid: target_pid}) do
      send(target_pid, {:message_handled, message})
      {:ok, message}
    end

    def handle_batch(publisher, batch, %{handle_batch: handler} = context) do
      handler.(publisher, batch, context)
    end

    def handle_batch(publisher, batch, %{target_pid: target_pid}) do
      send(target_pid, {:batch_handled, publisher, batch.messages})
      {:ack, successful: batch.messages, failed: []}
    end
  end

  describe "processor" do
    test "handle all produced messages" do
      {:ok, _} =
        Broadway.start_link(Forwarder, %{target_pid: self()},
          name: new_unique_name(),
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
        Broadway.start_link(Forwarder, %{target_pid: self()},
          name: new_unique_name(),
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
        Broadway.start_link(Forwarder, %{target_pid: self()},
          name: new_unique_name(),
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
        Broadway.start_link(Forwarder, %{target_pid: self()},
          name: new_unique_name(),
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
        Broadway.start_link(Forwarder, %{target_pid: self()},
          name: new_unique_name(),
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
        Broadway.start_link(Forwarder, %{target_pid: self()},
          name: new_unique_name(),
          producers: [[module: Counter, arg: [from: 1, to: 200]]],
          publishers: [:odd, :even]
        )

      assert_receive {:ack, successful, _failed}
      assert length(successful) == 100
    end

    test "messages including extra data for acknowledgement" do
      {:ok, _} =
        Broadway.start_link(Forwarder, %{target_pid: self()},
          name: new_unique_name(),
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
        Broadway.start_link(ForwarderWithNoPublisherDefined, %{target_pid: self()},
          name: new_unique_name(),
          producers: [[module: Counter, arg: [from: 1, to: 100]]]
        )

      assert_receive {:batch_handled, :default, _messages}
    end

    test "default number of processors is schedulers_online * 2" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder, %{target_pid: self()},
        name: broadway,
        producers: []
      )

      schedulers_online = :erlang.system_info(:schedulers_online)

      assert get_n_processors(broadway) == schedulers_online * 2
    end

    test "set number of processors" do
      broadway1 = new_unique_name()

      Broadway.start_link(Forwarder, %{target_pid: self()},
        name: broadway1,
        processors: [stages: 5],
        producers: []
      )

      broadway2 = new_unique_name()

      Broadway.start_link(Forwarder, %{target_pid: self()},
        name: broadway2,
        processors: [stages: 10],
        producers: []
      )

      assert get_n_processors(broadway1) == 5
      assert get_n_processors(broadway2) == 10
    end
  end

  describe "handle processor crash" do
    setup do
      handle_message = fn message, %{target_pid: target_pid} ->
        if message.data == 5 do
          Process.exit(message.processor_pid, :kill)
        end

        send(target_pid, {:message_handled, message})
        {:ok, message}
      end

      context = %{
        target_pid: self(),
        handle_message: handle_message
      }

      broadway_name = new_unique_name()

      {:ok, _pid} =
        Broadway.start_link(ForwarderWithCustomHandlers, context,
          name: broadway_name,
          processors: [stages: 1],
          producers: [[module: Counter, arg: [from: 1, to: 20]]],
          publishers: [default: [batch_size: 8]]
        )

      %{broadway_name: broadway_name}
    end

    test "processor will be restarted in order to handle other messages" do
      assert_receive {:message_handled, %{data: 4, processor_pid: processor1}}
      assert_receive {:message_handled, %{data: 9, processor_pid: processor2}}
      assert processor1 != processor2
    end

    test "batchers should not be restarted", %{broadway_name: broadway_name} do
      batcher_name = :"#{broadway_name}.Batcher_default"
      batcher_pid = Process.whereis(batcher_name)

      assert_receive {:batch_handled, _, _}
      assert_receive {:batch_handled, _, _}

      assert batcher_pid == Process.whereis(batcher_name)
    end

    test "only the messages in the crashing processor are lost" do
      for counter <- 1..4 do
        assert_receive {:message_handled, %{data: ^counter}}
      end

      for counter <- 5..8 do
        refute_receive {:message_handled, %{data: ^counter}}
      end

      for counter <- 9..20 do
        assert_receive {:message_handled, %{data: ^counter}}
      end
    end

    test "batches are created normally (without the lost messages)" do
      assert_receive {:batch_handled, _, messages}
      values = messages |> Enum.map(& &1.data)
      assert values == [1, 2, 3, 4, 9, 10, 11, 12]

      assert_receive {:batch_handled, _, messages}
      values = messages |> Enum.map(& &1.data)
      assert values == [13, 14, 15, 16, 17, 18, 19, 20]
    end
  end

  defp new_unique_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp get_n_processors(broadway_name) do
    Supervisor.count_children(:"#{broadway_name}.ProcessorSupervisor").workers
  end
end
