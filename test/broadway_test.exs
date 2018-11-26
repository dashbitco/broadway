defmodule BroadwayTest do
  use ExUnit.Case

  import Integer
  alias Broadway.{Producer, Message, BatchInfo}

  defmodule Counter do
    use GenStage

    @behaviour Broadway.Acknowledger

    def start_link(args, opts \\ []) do
      GenStage.start_link(__MODULE__, args, opts)
    end

    def init(args) do
      from = Keyword.fetch!(args, :from)
      to = Keyword.fetch!(args, :to)
      test_pid = Keyword.get(args, :test_pid)
      {:producer, %{to: to, counter: from, test_pid: test_pid}}
    end

    def handle_demand(demand, %{to: to, counter: counter} = state)
        when demand > 0 and counter > to do
      {:noreply, [], state}
    end

    def handle_demand(demand, state) when demand > 0 do
      %{to: to, counter: counter, test_pid: test_pid} = state
      max = min(to, counter + demand - 1)

      events =
        Enum.map(counter..max, fn e ->
          %Message{data: e, acknowledger: {__MODULE__, %{id: e, test_pid: test_pid}}}
        end)

      {:noreply, events, %{state | counter: max + 1}}
    end

    def ack(successful, failed) do
      [%Message{acknowledger: {_, %{test_pid: test_pid}}} | _] = successful

      if test_pid do
        send(test_pid, {:ack, successful, failed})
      end
    end
  end

  defmodule Forwarder do
    use Broadway

    import Message.Actions

    def handle_message(%Message{data: data} = message, %{test_pid: test_pid})
        when is_odd(data) do
      send(test_pid, {:message_handled, message.data})

      {:ok,
       message
       |> update_data(fn data -> data + 1000 end)
       |> put_publisher(:odd)}
    end

    def handle_message(message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data})

      {:ok,
       message
       |> update_data(fn data -> data + 1000 end)
       |> put_publisher(:even)}
    end

    def handle_batch(publisher, messages, _, %{test_pid: test_pid}) do
      send(test_pid, {:batch_handled, publisher, messages})
      {:ack, successful: messages, failed: []}
    end
  end

  defmodule ForwarderWithNoPublisherDefined do
    use Broadway

    def handle_message(message, _context) do
      {:ok, message}
    end

    def handle_batch(publisher, messages, _, %{test_pid: test_pid}) do
      send(test_pid, {:batch_handled, publisher, messages})
      {:ack, successful: messages, failed: []}
    end
  end

  defmodule ForwarderWithCustomHandlers do
    use Broadway

    def handle_message(message, %{handle_message: handler} = context) do
      handler.(message, context)
    end

    def handle_message(message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message})
      {:ok, message}
    end

    def handle_batch(publisher, messages, batch_info, %{handle_batch: handler} = context) do
      handler.(publisher, messages, batch_info, context)
    end

    def handle_batch(publisher, messages, batch_info, %{test_pid: test_pid}) do
      send(test_pid, {:batch_handled, publisher, messages, batch_info})
      {:ack, successful: messages, failed: []}
    end
  end

  describe "producer" do
    test "multiple producers" do
      {:ok, _} =
        Broadway.start_link(Forwarder, %{test_pid: self()},
          name: new_unique_name(),
          producers: [
            counter1: [module: Counter, arg: [from: 1, to: 20]],
            counter2: [module: Counter, arg: [from: 1, to: 20]]
          ],
          publishers: [:even, :odd]
        )

      for counter <- 1..20 do
        assert_receive {:message_handled, ^counter}
        assert_receive {:message_handled, ^counter}
      end

      refute_receive {:message_handled, _}
    end

    test "push_message/2" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder, %{test_pid: self()},
        name: broadway,
        producers: [
          counter1: [module: Counter, arg: [from: 1, to: 20]]
        ],
        publishers: [:even, :odd]
      )

      producer = :"#{broadway}.Producer_counter1"
      message1 = %Message{data: 21, acknowledger: {Counter, %{id: 21, test_pid: self()}}}
      message2 = %Message{data: 22, acknowledger: {Counter, %{id: 22, test_pid: self()}}}
      Producer.push_message(producer, message1)
      Producer.push_message(producer, message2)

      for counter <- 1..22 do
        assert_receive {:message_handled, ^counter}
      end

      refute_receive {:message_handled, _}
    end
  end

  describe "processor" do
    test "handle all produced messages" do
      {:ok, _} =
        Broadway.start_link(Forwarder, %{test_pid: self()},
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
        Broadway.start_link(Forwarder, %{test_pid: self()},
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
        Broadway.start_link(Forwarder, %{test_pid: self()},
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
        Broadway.start_link(Forwarder, %{test_pid: self()},
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
        Broadway.start_link(Forwarder, %{test_pid: self()},
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
        Broadway.start_link(Forwarder, %{test_pid: self()},
          name: new_unique_name(),
          producers: [[module: Counter, arg: [from: 1, to: 200, test_pid: self()]]],
          publishers: [:odd, :even]
        )

      assert_receive {:ack, successful, _failed}
      assert length(successful) == 100
    end

    test "messages including extra data for acknowledgement" do
      {:ok, _} =
        Broadway.start_link(Forwarder, %{test_pid: self()},
          name: new_unique_name(),
          producers: [[module: Counter, arg: [from: 1, to: 200, test_pid: self()]]],
          publishers: [:odd, :even]
        )

      assert_receive {:ack, successful, _failed}

      assert Enum.all?(successful, fn %Message{acknowledger: {_, ack_data}, data: data} ->
               data == ack_data.id + 1000
             end)
    end

    test "define a default publisher when none is defined" do
      {:ok, _} =
        Broadway.start_link(ForwarderWithNoPublisherDefined, %{test_pid: self()},
          name: new_unique_name(),
          producers: [[module: Counter, arg: [from: 1, to: 100]]]
        )

      assert_receive {:batch_handled, :default, _messages}
    end

    test "default number of processors is schedulers_online * 2" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder, %{test_pid: self()},
        name: broadway,
        producers: []
      )

      schedulers_online = :erlang.system_info(:schedulers_online)

      assert get_n_processors(broadway) == schedulers_online * 2
    end

    test "set number of processors" do
      broadway1 = new_unique_name()

      Broadway.start_link(Forwarder, %{test_pid: self()},
        name: broadway1,
        processors: [stages: 5],
        producers: []
      )

      broadway2 = new_unique_name()

      Broadway.start_link(Forwarder, %{test_pid: self()},
        name: broadway2,
        processors: [stages: 10],
        producers: []
      )

      assert get_n_processors(broadway1) == 5
      assert get_n_processors(broadway2) == 10
    end

    test "default number of consumers is 1 per publisher" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder, %{test_pid: self()},
        name: broadway,
        producers: [],
        publishers: [:p1, :p2]
      )

      assert get_n_consumers(broadway, :p1) == 1
      assert get_n_consumers(broadway, :p2) == 1
    end

    test "set number of consumers" do
      broadway1 = new_unique_name()

      Broadway.start_link(Forwarder, %{test_pid: self()},
        name: broadway1,
        producers: [],
        publishers: [
          p1: [stages: 2],
          p2: [stages: 3]
        ]
      )

      broadway2 = new_unique_name()

      Broadway.start_link(Forwarder, %{test_pid: self()},
        name: broadway2,
        producers: [],
        publishers: [
          p1: [stages: 4],
          p2: [stages: 6]
        ]
      )

      assert get_n_consumers(broadway1, :p1) == 2
      assert get_n_consumers(broadway1, :p2) == 3

      assert get_n_consumers(broadway2, :p1) == 4
      assert get_n_consumers(broadway2, :p2) == 6
    end
  end

  describe "handle processor crash" do
    setup do
      handle_message = fn message, %{test_pid: test_pid} ->
        if message.data == 3 do
          Process.exit(message.processor_pid, :kill)
        end

        send(test_pid, {:message_handled, message})
        {:ok, message}
      end

      context = %{
        test_pid: self(),
        handle_message: handle_message
      }

      broadway_name = new_unique_name()

      {:ok, _pid} =
        Broadway.start_link(ForwarderWithCustomHandlers, context,
          name: broadway_name,
          processors: [stages: 1, min_demand: 1, max_demand: 2],
          producers: [[module: Counter, arg: [from: 1, to: 6]]],
          publishers: [default: [batch_size: 2]]
        )

      %{broadway_name: broadway_name}
    end

    test "processor will be restarted in order to handle other messages" do
      assert_receive {:message_handled, %{data: 1, processor_pid: processor1}}
      assert_receive {:message_handled, %{data: 5, processor_pid: processor2}}
      assert processor1 != processor2
    end

    test "batchers should not be restarted", %{broadway_name: broadway_name} do
      batcher_name = :"#{broadway_name}.Batcher_default"
      batcher_pid = Process.whereis(batcher_name)

      assert_receive {:batch_handled, _, _, _}
      assert_receive {:batch_handled, _, _, _}

      assert batcher_pid == Process.whereis(batcher_name)
    end

    test "only the messages in the crashing processor are lost" do
      assert_receive {:message_handled, %{data: 1}}
      assert_receive {:message_handled, %{data: 2}}

      refute_receive {:message_handled, %{data: 3}}
      refute_receive {:message_handled, %{data: 4}}

      assert_receive {:message_handled, %{data: 5}}
      assert_receive {:message_handled, %{data: 6}}
    end

    test "batches are created normally (without the lost messages)" do
      assert_receive {:batch_handled, _, messages, _}
      values = messages |> Enum.map(& &1.data)
      assert values == [1, 2]

      assert_receive {:batch_handled, _, messages, _}
      values = messages |> Enum.map(& &1.data)
      assert values == [5, 6]
    end
  end

  describe "handle batcher crash" do
    setup do
      handle_batch = fn _publisher, messages, batch_info, %{test_pid: test_pid} ->
        if Enum.at(messages, 0).data == 1 do
          Process.exit(batch_info.batcher, :kill)
        end

        send(test_pid, {:batch_handled, messages, batch_info})
        {:ack, successful: messages, failed: []}
      end

      context = %{
        test_pid: self(),
        handle_batch: handle_batch
      }

      {:ok, _pid} =
        Broadway.start_link(ForwarderWithCustomHandlers, context,
          name: new_unique_name(),
          processors: [stages: 1, min_demand: 1, max_demand: 2],
          producers: [[module: Counter, arg: [from: 1, to: 6, test_pid: self()]]],
          publishers: [default: [batch_size: 2, min_demand: 0, max_demand: 2]]
        )

      :ok
    end

    test "batcher will be restarted in order to handle other messages" do
      assert_receive {:batch_handled, _, %BatchInfo{batcher: batcher1}}
      assert_receive {:batch_handled, _, %BatchInfo{batcher: batcher2}}
      assert batcher1 != batcher2
    end

    test "only the messages in the crashing batcher are lost" do
      assert_receive {:ack, successful, _failed}
      values = Enum.map(successful, & &1.data)
      assert values == [1, 2]

      assert_receive {:ack, successful, _failed}
      values = Enum.map(successful, & &1.data)
      assert values == [5, 6]

      refute_receive {:ack, _successful, _failed}
    end
  end

  describe "shutdown" do
    test "shutting down broadway waits until the Broadway.Supervisor is down" do
      Process.flag(:trap_exit, true)

      {:ok, pid} =
        Broadway.start_link(ForwarderWithNoPublisherDefined, %{test_pid: self()},
          name: new_unique_name(),
          producers: [[module: Counter, arg: [from: 1, to: 10]]]
        )

      %Broadway.State{supervisor_pid: supervisor_pid} = :sys.get_state(pid)

      Process.exit(pid, :shutdown)

      assert_receive {:EXIT, ^pid, :shutdown}
      assert Process.alive?(supervisor_pid) == false
    end

    @tag :capture_log
    test "killing the supervisor brings down the broadway GenServer" do
      Process.flag(:trap_exit, true)

      {:ok, pid} =
        Broadway.start_link(ForwarderWithNoPublisherDefined, %{test_pid: self()},
          name: new_unique_name(),
          producers: [[module: Counter, arg: [from: 1, to: 10]]]
        )

      %Broadway.State{supervisor_pid: supervisor_pid} = :sys.get_state(pid)

      Process.exit(supervisor_pid, :kill)
      assert_receive {:EXIT, ^pid, :killed}
    end
  end

  describe "use Broadway" do
    test "generates child_spec/1" do
      defmodule MyBroadway do
        use Broadway
        def handle_message(_, _), do: nil
        def handle_batch(_, _, _, _), do: nil
      end

      assert MyBroadway.child_spec(:arg) == %{
               id: BroadwayTest.MyBroadway,
               start: {Broadway, :start_link, [BroadwayTest.MyBroadway, %{}, :arg]}
             }
    end

    test "generates child_spec/1 with overriden options" do
      defmodule BroadwayWithCustomOptions do
        use Broadway, id: :some_id

        def handle_message(_, _), do: nil
        def handle_batch(_, _, _, _), do: nil
      end

      assert BroadwayWithCustomOptions.child_spec(:arg).id == :some_id
    end
  end

  defp new_unique_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp get_n_processors(broadway_name) do
    Supervisor.count_children(:"#{broadway_name}.ProcessorSupervisor").workers
  end

  defp get_n_consumers(broadway_name, key) do
    Supervisor.count_children(:"#{broadway_name}.ConsumerSupervisor_#{key}").workers
  end
end
