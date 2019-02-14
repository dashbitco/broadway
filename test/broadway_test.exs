defmodule BroadwayTest do
  use ExUnit.Case

  import Integer
  import ExUnit.CaptureLog

  alias Broadway.{Producer, Message, BatchInfo}

  defmodule Acker do
    @behaviour Broadway.Acknowledger

    def ack(successful, failed) do
      test_pid =
        case {successful, failed} do
          {[%Message{acknowledger: {_, %{test_pid: pid}}} | _], _failed} ->
            pid

          {_successful, [%Message{acknowledger: {_, %{test_pid: pid}}} | _]} ->
            pid
        end

      send(test_pid, {:ack, successful, failed})
    end
  end

  defmodule ManualProducer do
    use GenStage

    def start_link(args, opts \\ []) do
      GenStage.start_link(__MODULE__, args, opts)
    end

    def init(%{test_pid: test_pid}) do
      name = Process.info(self())[:registered_name]
      send(test_pid, {:producer_initialized, name})
      {:producer, %{test_pid: test_pid}}
    end

    def init(_args) do
      {:producer, %{}}
    end

    def handle_demand(_demand, state) do
      {:noreply, [], state}
    end
  end

  defmodule EventProducer do
    use GenStage

    def start_link(events) do
      GenStage.start_link(__MODULE__, events)
    end

    def init(events) do
      {:producer, events}
    end

    def handle_demand(demand, events) when demand > 0 do
      {events, rest} = Enum.split(events, demand)
      {:noreply, events, rest}
    end
  end

  defmodule Forwarder do
    use Broadway

    import Message

    def handle_message(:default, %Message{data: data} = message, %{test_pid: test_pid})
        when is_odd(data) do
      send(test_pid, {:message_handled, message.data})

      message
      |> update_data(fn data -> data + 1000 end)
      |> put_batcher(:odd)
    end

    def handle_message(:default, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data})

      message
      |> update_data(fn data -> data + 1000 end)
      |> put_batcher(:even)
    end

    def handle_batch(batcher, messages, _, %{test_pid: test_pid}) do
      send(test_pid, {:batch_handled, batcher, messages})
      messages
    end
  end

  defmodule ForwarderWithCustomHandlers do
    use Broadway

    def handle_message(_, message, %{handle_message: handler} = context) do
      handler.(message, context)
    end

    def handle_batch(batcher, messages, batch_info, %{handle_batch: handler} = context) do
      handler.(batcher, messages, batch_info, context)
    end
  end

  defmodule Transformer do
    def transform(event, test_pid: test_pid) do
      if event == :kill_producer do
        raise "Error raised"
      end

      %Message{
        data: "#{event} transformed",
        acknowledger: {Acker, %{id: event, test_pid: test_pid}}
      }
    end
  end

  describe "use Broadway" do
    test "generates child_spec/1" do
      defmodule MyBroadway do
        use Broadway
        def handle_message(_, _, _), do: nil
        def handle_batch(_, _, _, _), do: nil
      end

      assert MyBroadway.child_spec(:arg) == %{
               id: BroadwayTest.MyBroadway,
               start: {BroadwayTest.MyBroadway, :start_link, [:arg]},
               type: :supervisor
             }
    end

    test "generates child_spec/1 with overriden options" do
      defmodule MyBroadwayWithCustomOptions do
        use Broadway, id: :some_id

        def handle_message(_, _, _), do: nil
        def handle_batch(_, _, _, _), do: nil
      end

      assert MyBroadwayWithCustomOptions.child_spec(:arg).id == :some_id
    end
  end

  describe "broadway configuration" do
    test "invalid configuration options" do
      assert_raise ArgumentError,
                   "invalid configuration given to Broadway.start_link/2, expected :name to be an atom, got: 1",
                   fn -> Broadway.start_link(Forwarder, name: 1) end
    end

    test "default number of producers is 1" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        context: %{test_pid: self()},
        producers: [
          default: [module: ManualProducer, arg: []]
        ],
        processors: [default: []],
        batchers: [default: []]
      )

      assert get_n_producers(broadway) == 1
    end

    test "set number of producers" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        context: %{test_pid: self()},
        producers: [
          default: [module: ManualProducer, arg: [], stages: 3]
        ],
        processors: [default: []],
        batchers: [default: []]
      )

      assert get_n_producers(broadway) == 3
    end

    test "default number of processors is schedulers_online * 2" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        producers: [
          default: [module: ManualProducer, arg: []]
        ],
        processors: [default: []],
        batchers: [default: []]
      )

      assert get_n_processors(broadway) == System.schedulers_online() * 2
    end

    test "set number of processors" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        producers: [
          default: [module: ManualProducer, arg: []]
        ],
        processors: [default: [stages: 13]],
        batchers: [default: []]
      )

      assert get_n_processors(broadway) == 13
    end

    test "default number of consumers is 1 per batcher" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        context: %{test_pid: self()},
        producers: [
          default: [module: ManualProducer, arg: []]
        ],
        processors: [default: []],
        batchers: [p1: [], p2: []]
      )

      assert get_n_consumers(broadway, :p1) == 1
      assert get_n_consumers(broadway, :p2) == 1
    end

    test "set number of consumers" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        context: %{test_pid: self()},
        producers: [
          default: [module: ManualProducer, arg: []]
        ],
        processors: [default: []],
        batchers: [
          p1: [stages: 2],
          p2: [stages: 3]
        ]
      )

      assert get_n_consumers(broadway, :p1) == 2
      assert get_n_consumers(broadway, :p2) == 3
    end

    test "default context is :context_not_set" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        producers: [
          default: [module: ManualProducer, arg: []]
        ],
        processors: [default: []],
        batchers: [default: []]
      )

      pid = get_processor(broadway, :default)
      assert :sys.get_state(pid).state.context == :context_not_set
    end
  end

  test "push_messages/2" do
    {:ok, pid} =
      Broadway.start_link(Forwarder,
        name: new_unique_name(),
        context: %{test_pid: self()},
        producers: [
          default: [module: ManualProducer, arg: []]
        ],
        processors: [default: []],
        batchers: [
          even: [],
          odd: []
        ]
      )

    Broadway.push_messages(pid, [
      %Message{data: 1, acknowledger: {__MODULE__, 1}},
      %Message{data: 3, acknowledger: {__MODULE__, 3}}
    ])

    assert_receive {:message_handled, 1}
    assert_receive {:message_handled, 3}
  end

  describe "processor" do
    setup do
      broadway = new_unique_name()

      {:ok, _pid} =
        Broadway.start_link(Forwarder,
          name: broadway,
          context: %{test_pid: self()},
          producers: [
            default: [module: ManualProducer, arg: []]
          ],
          processors: [default: []],
          batchers: [
            even: [],
            odd: []
          ]
        )

      %{producer: get_producer(broadway)}
    end

    test "handle all produced messages", %{producer: producer} do
      push_messages(producer, 1..200)

      for counter <- 1..200 do
        assert_receive {:message_handled, ^counter}
      end

      refute_receive {:message_handled, _}
    end

    test "forward messages to the specified batcher", %{producer: producer} do
      push_messages(producer, 1..200)

      assert_receive {:batch_handled, :odd, messages}
      assert Enum.all?(messages, fn msg -> is_odd(msg.data) end)

      assert_receive {:batch_handled, :even, messages}
      assert Enum.all?(messages, fn msg -> is_even(msg.data) end)

      refute_receive {:batch_handled, _, _}
    end
  end

  describe "processor - failed messages handling" do
    setup do
      test_pid = self()

      handle_message = fn message, _ ->
        case message.data do
          :fail -> Message.failed(message, "Failed message")
          :raise -> raise "Error raised"
          :bad_return -> :oops
          :bad_batcher -> %{message | batcher: :unknown}
          _ -> message
        end
      end

      handle_batch = fn _, batch, _, _ ->
        send(test_pid, {:batch_handled, batch})
        batch
      end

      context = %{
        handle_message: handle_message,
        handle_batch: handle_batch
      }

      broadway_name = new_unique_name()

      {:ok, _pid} =
        Broadway.start_link(ForwarderWithCustomHandlers,
          name: broadway_name,
          context: context,
          producers: [
            default: [module: ManualProducer, arg: []]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      producer = get_producer(broadway_name, :default)
      processor = get_processor(broadway_name, :default)
      Process.link(Process.whereis(processor))

      %{producer: producer, processor: processor}
    end

    test "successful messages are marked as :ok", %{producer: producer} do
      push_messages(producer, [1, 2])
      assert_receive {:batch_handled, [%{status: :ok}, %{status: :ok}]}
    end

    test "failed messages are marked as {:failed, reason}", %{producer: producer} do
      push_messages(producer, [:fail])
      assert_receive {:ack, _, [%{status: {:failed, "Failed message"}}]}
    end

    test "failed messages are not forwarded to the batcher", %{producer: producer} do
      push_messages(producer, [1, :fail, :fail, 4])

      assert_receive {:batch_handled, batch}
      assert Enum.map(batch, & &1.data) == [1, 4]
    end

    test "messages are grouped as successful and failed before sent for acknowledgement",
         %{producer: producer} do
      push_messages(producer, [1, :fail, 4])

      assert_receive {:ack, [], [%{data: :fail}]}
      assert_receive {:ack, [%{data: 1}, %{data: 4}], []}
    end

    test "messages are marked as {:failed, reason} when an error is raised while processing",
         %{producer: producer, processor: processor} do
      assert capture_log(fn ->
               push_messages(producer, [1, :raise, 4])
               assert_receive {:ack, _, [%{status: {:failed, "due to an unhandled error"}}]}
               assert_receive {:ack, [%{data: 1}, %{data: 4}], []}
             end) =~ "[error] ** (RuntimeError) Error raised"

      refute_received {:EXIT, _, ^processor}
    end

    test "messages are marked as {:failed, reason} on bad return",
         %{producer: producer, processor: processor} do
      assert capture_log(fn ->
               push_messages(producer, [1, :bad_return, 4])
               assert_receive {:ack, _, [%{status: {:failed, "due to an unhandled error"}}]}
               assert_receive {:ack, [%{data: 1}, %{data: 4}], []}
             end) =~
               "[error] ** (RuntimeError) expected a Broadway.Message from handle_message/3, got :oops"

      refute_received {:EXIT, _, ^processor}
    end

    test "messages are marked as {:failed, reason} on bad batcher",
         %{producer: producer, processor: processor} do
      assert capture_log(fn ->
               push_messages(producer, [1, :bad_batcher, 4])
               assert_receive {:ack, _, [%{status: {:failed, "due to an unhandled error"}}]}
             end) =~
               "[error] ** (RuntimeError) message was set to unknown batcher :unknown. The known batchers are [:default]"

      refute_received {:EXIT, _, ^processor}
    end

    test "processors do not crash on bad acknowledger",
         %{producer: producer, processor: processor} do
      [one, raise, four] = wrap_messages([1, :raise, 4])
      raise = %{raise | acknowledger: {Unknown, :ok}}

      assert capture_log(fn ->
               Producer.push_messages(producer, [one, raise, four])
               assert_receive {:ack, [%{data: 1}, %{data: 4}], []}
             end) =~ "[error] ** (UndefinedFunctionError) function Unknown.ack/2 is undefined"

      refute_received {:EXIT, _, ^processor}
    end
  end

  describe "batcher" do
    setup do
      broadway = new_unique_name()

      {:ok, _pid} =
        Broadway.start_link(Forwarder,
          name: broadway,
          context: %{test_pid: self()},
          producers: [
            default: [module: ManualProducer, arg: []]
          ],
          processors: [default: []],
          batchers: [
            odd: [batch_size: 10, batch_timeout: 20],
            even: [batch_size: 5, batch_timeout: 20]
          ]
        )

      %{producer: get_producer(broadway)}
    end

    test "generate batches based on :batch_size", %{producer: producer} do
      push_messages(producer, 1..40)

      assert_receive {:batch_handled, :odd, messages} when length(messages) == 10
      assert_receive {:batch_handled, :odd, messages} when length(messages) == 10
      assert_receive {:batch_handled, :even, messages} when length(messages) == 5
      assert_receive {:batch_handled, :even, messages} when length(messages) == 5
      assert_receive {:batch_handled, :even, messages} when length(messages) == 5
      assert_receive {:batch_handled, :even, messages} when length(messages) == 5
      refute_receive {:batch_handled, _, _}
    end

    test "generate batches with the remaining messages after :batch_timeout is reached",
         %{producer: producer} do
      push_messages(producer, 1..5)

      assert_receive {:batch_handled, :odd, messages} when length(messages) == 3
      assert_receive {:batch_handled, :even, messages} when length(messages) == 2
      refute_receive {:batch_handled, _, _}
    end

    test "pass all messages to the acknowledger", %{producer: producer} do
      push_messages(producer, [1, 3, 5, 7, 9, 11, 13, 15, 17, 19])

      assert_receive {:ack, successful, _failed}
      assert length(successful) == 10

      push_messages(producer, [2, 4, 6, 8, 10])

      assert_receive {:ack, successful, _failed}
      assert length(successful) == 5
    end

    test "pass all messages to the acknowledger, including extra data", %{producer: producer} do
      push_messages(producer, [2, 4, 6, 8, 10])

      assert_receive {:ack, successful, _failed}

      assert Enum.all?(successful, fn %Message{acknowledger: {_, ack_data}, data: data} ->
               data == ack_data.id + 1000
             end)
    end
  end

  describe "partition" do
    setup do
      broadway = new_unique_name()

      handle_message = fn message, _ ->
        if is_odd(message.data) do
          Message.put_partition(message, :odd)
        else
          Message.put_partition(message, :even)
        end
      end

      context = %{
        handle_message: handle_message,
        handle_batch: fn _, batch, _, _ -> batch end
      }

      {:ok, _pid} =
        Broadway.start_link(ForwarderWithCustomHandlers,
          name: broadway,
          context: context,
          producers: [default: [module: ManualProducer, arg: []]],
          processors: [default: []],
          batchers: [default: [batch_size: 2, batch_timeout: 20]]
        )

      %{producer: get_producer(broadway)}
    end

    test "generate batches based on :batch_size", %{producer: producer} do
      push_messages(producer, 1..10)

      assert_receive {:ack, [%{data: 1, partition: :odd}, %{data: 3, partition: :odd}], []}
      assert_receive {:ack, [%{data: 2, partition: :even}, %{data: 4, partition: :even}], []}
    end

    test "generate batches with the remaining messages after :batch_timeout is reached",
         %{producer: producer} do
      push_messages(producer, 1..2)

      assert_receive {:ack, [%{data: 1, partition: :odd}], []}
      assert_receive {:ack, [%{data: 2, partition: :even}], []}
    end
  end

  describe "transformer" do
    setup tags do
      test_pid = self()

      handle_message = fn message, _ ->
        send(test_pid, {:message_handled, message})
        message
      end

      context = %{
        handle_message: handle_message,
        handle_batch: fn _, batch, _, _ -> batch end
      }

      broadway_name = new_unique_name()

      {:ok, _pid} =
        Broadway.start_link(ForwarderWithCustomHandlers,
          name: broadway_name,
          resubscribe_interval: 0,
          context: context,
          producers: [
            default: [
              module: EventProducer,
              arg: Map.get(tags, :events, [1, 2, 3]),
              transformer: {Transformer, :transform, test_pid: self()}
            ]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      producer = get_producer(broadway_name, :default)

      %{producer: producer}
    end

    test "transform all events" do
      assert_receive {:message_handled, %{data: "1 transformed"}}
      assert_receive {:message_handled, %{data: "2 transformed"}}
      assert_receive {:message_handled, %{data: "3 transformed"}}
    end

    @tag events: [1, 2, :kill_producer, 4]
    test "restart the producer if the transformation raises an error", context do
      %{producer: producer} = context
      ref_producer = Process.monitor(producer)

      assert_receive {:message_handled, %{data: "1 transformed"}}
      assert_receive {:message_handled, %{data: "2 transformed"}}
      assert_receive {:DOWN, ^ref_producer, _, _, _}
      assert_receive {:message_handled, %{data: "1 transformed"}}
      assert_receive {:message_handled, %{data: "2 transformed"}}
    end
  end

  describe "handle producer crash" do
    setup do
      test_pid = self()

      handle_message = fn message, _ ->
        send(test_pid, {:message_handled, message})
        message
      end

      context = %{
        handle_message: handle_message,
        handle_batch: fn _, batch, _, _ -> batch end
      }

      broadway_name = new_unique_name()

      {:ok, _pid} =
        Broadway.start_link(ForwarderWithCustomHandlers,
          name: broadway_name,
          resubscribe_interval: 0,
          context: context,
          producers: [
            default: [
              module: ManualProducer,
              arg: %{test_pid: self()}
            ]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      producer = get_producer(broadway_name, :default)
      processor = get_processor(broadway_name, :default)
      batcher = get_batcher(broadway_name, :default)
      consumer = get_consumer(broadway_name, :default)

      %{producer: producer, processor: processor, batcher: batcher, consumer: consumer}
    end

    test "only the producer will be restarted", context do
      %{producer: producer, processor: processor, batcher: batcher, consumer: consumer} = context

      ref_producer = Process.monitor(producer)
      ref_batcher = Process.monitor(batcher)
      ref_processor = Process.monitor(processor)
      ref_consumer = Process.monitor(consumer)

      GenStage.stop(producer)

      assert_receive {:DOWN, ^ref_producer, _, _, _}
      refute_receive {:DOWN, ^ref_processor, _, _, _}
      refute_receive {:DOWN, ^ref_batcher, _, _, _}
      refute_receive {:DOWN, ^ref_consumer, _, _, _}

      assert_receive {:producer_initialized, ^producer}
    end

    test "processors resubscribe to the restarted producers and keep processing messages" do
      assert_receive {:producer_initialized, producer}

      push_messages(producer, [1])
      assert_receive {:message_handled, %{data: 1}}

      GenStage.stop(producer)
      assert_receive {:producer_initialized, ^producer}

      push_messages(producer, [2])
      assert_receive {:message_handled, %{data: 2}}
    end
  end

  describe "handle processor crash" do
    setup do
      test_pid = self()

      handle_message = fn message, _ ->
        if message.data == :kill_processor do
          Process.exit(self(), :kill)
        end

        send(test_pid, {:message_handled, message})
        message
      end

      handle_batch = fn _, batch, _, _ ->
        send(test_pid, {:batch_handled, batch})
        batch
      end

      context = %{
        handle_message: handle_message,
        handle_batch: handle_batch
      }

      broadway_name = new_unique_name()

      {:ok, _pid} =
        Broadway.start_link(ForwarderWithCustomHandlers,
          name: broadway_name,
          context: context,
          producers: [
            default: [module: ManualProducer, arg: []]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      producer = get_producer(broadway_name, :default)
      processor = get_processor(broadway_name, :default)
      batcher = get_batcher(broadway_name, :default)
      consumer = get_consumer(broadway_name, :default)

      %{producer: producer, processor: processor, batcher: batcher, consumer: consumer}
    end

    test "processor will be restarted in order to handle other messages",
         %{producer: producer, processor: processor} do
      push_messages(producer, [1])
      assert_receive {:message_handled, %{data: 1}}

      pid = Process.whereis(processor)
      ref = Process.monitor(processor)
      push_messages(producer, [:kill_processor])
      assert_receive {:DOWN, ^ref, _, _, _}

      push_messages(producer, [2])
      assert_receive {:message_handled, %{data: 2}}
      assert Process.whereis(processor) != pid
    end

    test "processor crashes cascade down (but not up)", context do
      %{producer: producer, processor: processor, batcher: batcher, consumer: consumer} = context

      ref_batcher = Process.monitor(batcher)
      ref_processor = Process.monitor(processor)
      ref_producer = Process.monitor(producer)
      ref_consumer = Process.monitor(consumer)

      push_messages(producer, [:kill_processor])

      assert_receive {:DOWN, ^ref_processor, _, _, _}
      assert_receive {:DOWN, ^ref_batcher, _, _, _}
      assert_receive {:DOWN, ^ref_consumer, _, _, _}
      refute_received {:DOWN, ^ref_producer, _, _, _}
    end

    test "only the messages in the crashing processor are lost", %{producer: producer} do
      push_messages(producer, [1, 2, :kill_processor, 3, 4, 5])

      assert_receive {:message_handled, %{data: 1}}
      assert_receive {:message_handled, %{data: 2}}

      refute_receive {:message_handled, %{data: :kill_processor}}
      refute_receive {:message_handled, %{data: 3}}

      assert_receive {:message_handled, %{data: 4}}
      assert_receive {:message_handled, %{data: 5}}
    end

    test "batches are created normally (without the lost messages)", %{producer: producer} do
      push_messages(producer, [1, 2, :kill_processor, 3, 4, 5])

      assert_receive {:batch_handled, messages}
      values = messages |> Enum.map(& &1.data)
      assert values == [1, 2]

      assert_receive {:batch_handled, messages}
      values = messages |> Enum.map(& &1.data)
      assert values == [4, 5]
    end
  end

  describe "handle batcher crash" do
    setup do
      test_pid = self()

      handle_batch = fn _publisher, messages, batch_info, _ ->
        if Enum.any?(messages, fn msg -> msg.data == :kill_batcher end) do
          Process.exit(batch_info.batcher_pid, :kill)
        end

        send(test_pid, {:batch_handled, messages, batch_info})
        messages
      end

      context = %{
        handle_batch: handle_batch,
        handle_message: fn message, _ -> message end
      }

      broadway_name = new_unique_name()

      {:ok, _pid} =
        Broadway.start_link(ForwarderWithCustomHandlers,
          name: broadway_name,
          context: context,
          producers: [
            default: [module: ManualProducer, arg: []]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      producer = get_producer(broadway_name, :default)
      processor = get_processor(broadway_name, :default)
      batcher = get_batcher(broadway_name, :default)
      consumer = get_consumer(broadway_name, :default)

      %{producer: producer, processor: processor, batcher: batcher, consumer: consumer}
    end

    test "batcher will be restarted in order to handle other messages", %{producer: producer} do
      push_messages(producer, [1, 2])
      assert_receive {:batch_handled, _, %BatchInfo{batcher_pid: batcher1}}

      ref = Process.monitor(batcher1)

      push_messages(producer, [:kill_batcher, 3])
      assert_receive {:batch_handled, _, %BatchInfo{batcher_pid: ^batcher1}}

      assert_receive {:DOWN, ^ref, _, obj, _}

      push_messages(producer, [4, 5])
      assert_receive {:batch_handled, _, %BatchInfo{batcher_pid: batcher2}}

      assert batcher1 != batcher2
    end

    test "batcher crashes cascade down (but not up)", context do
      %{producer: producer, processor: processor, batcher: batcher, consumer: consumer} = context

      ref_batcher = Process.monitor(batcher)
      ref_processor = Process.monitor(processor)
      ref_producer = Process.monitor(producer)
      ref_consumer = Process.monitor(consumer)

      push_messages(producer, [:kill_batcher, 3])

      assert_receive {:DOWN, ^ref_batcher, _, _, _}
      assert_receive {:DOWN, ^ref_consumer, _, _, _}
      refute_received {:DOWN, ^ref_producer, _, _, _}
      refute_received {:DOWN, ^ref_processor, _, _, _}
    end

    test "only the messages in the crashing batcher are lost", %{producer: producer} do
      push_messages(producer, [1, 2, :kill_batcher, 3, 4, 5, 6, 7])

      assert_receive {:ack, successful, _failed}
      values = Enum.map(successful, & &1.data)
      assert values == [1, 2]

      assert_receive {:ack, successful, _failed}
      values = Enum.map(successful, & &1.data)
      assert values == [:kill_batcher, 3]

      assert_receive {:ack, successful, _failed}
      values = Enum.map(successful, & &1.data)
      assert values == [6, 7]

      refute_receive {:ack, _successful, _failed}
    end
  end

  describe "batcher - failed messages handling" do
    setup do
      test_pid = self()

      handle_batch = fn _, batch, _, _ ->
        send(test_pid, {:batch_handled, batch})

        Enum.map(batch, fn message ->
          case message.data do
            :fail -> Message.failed(message, "Failed message")
            :raise -> raise "Error raised"
            :bad_return -> :oops
            _ -> message
          end
        end)
      end

      context = %{
        handle_batch: handle_batch,
        handle_message: fn message, _ -> message end
      }

      broadway_name = new_unique_name()

      {:ok, _pid} =
        Broadway.start_link(ForwarderWithCustomHandlers,
          name: broadway_name,
          context: context,
          producers: [
            default: [module: ManualProducer, arg: []]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 4]],
          batchers: [default: [batch_size: 4]]
        )

      producer = get_producer(broadway_name, :default)
      consumer = get_consumer(broadway_name, :default)
      Process.link(Process.whereis(consumer))

      %{producer: producer, consumer: consumer}
    end

    test "messages are grouped as successful and failed before sent for acknowledgement",
         %{producer: producer} do
      push_messages(producer, [1, :fail, :fail, 4])

      assert_receive {:ack, [%{data: 1}, %{data: 4}], [%{data: :fail}, %{data: :fail}]}
    end

    test "all messages in the batch are marked as {:failed, reason} when an error is raised",
         %{producer: producer, consumer: consumer} do
      assert capture_log(fn ->
               push_messages(producer, [1, 2, :raise, 3])

               assert_receive {:ack, _,
                               [
                                 %{data: 1, status: {:failed, "due to an unhandled error"}},
                                 %{data: 2, status: {:failed, "due to an unhandled error"}},
                                 %{data: :raise, status: {:failed, "due to an unhandled error"}},
                                 %{data: 3, status: {:failed, "due to an unhandled error"}}
                               ]}
             end) =~ "[error] ** (RuntimeError) Error raised"

      refute_received {:EXIT, _, ^consumer}
    end

    test "all messages in the batch are marked as {:failed, reason} on bad return",
         %{producer: producer, consumer: consumer} do
      assert capture_log(fn ->
               push_messages(producer, [1, 2, :bad_return, 3])

               assert_receive {:ack, _,
                               [
                                 %{data: 1, status: {:failed, "due to an unhandled error"}},
                                 %{data: 2, status: {:failed, "due to an unhandled error"}},
                                 %{
                                   data: :bad_return,
                                   status: {:failed, "due to an unhandled error"}
                                 },
                                 %{data: 3, status: {:failed, "due to an unhandled error"}}
                               ]}
             end) =~ "[error]"

      refute_received {:EXIT, _, ^consumer}
    end

    test "consumers do not crash on bad acknowledger",
         %{producer: producer, consumer: consumer} do
      [one, two, raise, three] = wrap_messages([1, 2, :raise, 3])
      raise = %{raise | acknowledger: {Unknown, :ok}}

      assert capture_log(fn ->
               Producer.push_messages(producer, [one, two, raise, three])
               push_messages(producer, [1, 2, 3, 4])
               assert_receive {:ack, [%{data: 1}, %{data: 2}, %{data: 3}, %{data: 4}], []}
             end) =~
               "[error] ** (UndefinedFunctionError) function Unknown.ack/2 is undefined"

      refute_received {:EXIT, _, ^consumer}
    end
  end

  describe "shutdown" do
    setup tags do
      Process.flag(:trap_exit, true)
      broadway_name = new_unique_name()

      handle_message = fn
        %{data: :sleep}, _ -> Process.sleep(:infinity)
        message, _ -> message
      end

      context = %{
        handle_message: handle_message,
        handle_batch: fn _, batch, _, _ -> batch end
      }

      {:ok, pid} =
        Broadway.start_link(ForwarderWithCustomHandlers,
          name: broadway_name,
          producers: [
            default: [module: ManualProducer, arg: []]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 4]],
          batchers: [default: [batch_size: 4]],
          context: context,
          shutdown: Map.get(tags, :shutdown, 5000)
        )

      producer = get_producer(broadway_name, :default)
      %{broadway: pid, producer: producer}
    end

    test "killing the supervisor brings down the broadway GenServer",
         %{broadway: broadway} do
      %{supervisor_pid: supervisor_pid} = :sys.get_state(broadway)
      Process.exit(supervisor_pid, :kill)
      assert_receive {:EXIT, ^broadway, :killed}
    end

    test "shutting down broadway waits until the Broadway.Supervisor is down",
         %{broadway: broadway} do
      %{supervisor_pid: supervisor_pid} = :sys.get_state(broadway)
      Process.exit(broadway, :shutdown)

      assert_receive {:EXIT, ^broadway, :shutdown}
      refute Process.alive?(supervisor_pid)
    end

    test "shutting down broadway waits until all events are processed",
         %{broadway: broadway, producer: producer} do
      # We suspend the producer to make sure that it doesn't process the messages early on
      :sys.suspend(producer)
      async_push_messages(producer, [1, 2, 3, 4])
      Process.exit(broadway, :shutdown)
      :sys.resume(producer)
      assert_receive {:ack, [%{data: 1}, %{data: 2}, %{data: 3}, %{data: 4}], []}
    end

    test "shutting down broadway waits until all events are processed even on incomplete batches",
         %{broadway: broadway, producer: producer} do
      push_messages(producer, [1, 2])
      Process.exit(broadway, :shutdown)
      assert_receive {:ack, [%{data: 1}, %{data: 2}], []}
    end

    @tag shutdown: 1
    test "shutting down broadway respects shutdown value",
         %{broadway: broadway, producer: producer} do
      push_messages(producer, [:sleep, 1, 2, 3])
      Process.exit(broadway, :shutdown)
      assert_receive {:EXIT, ^broadway, :shutdown}
    end
  end

  defp new_unique_name() do
    :"Elixir.Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp get_producer(broadway_name, key \\ :default, index \\ 1) do
    :"#{broadway_name}.Producer_#{key}_#{index}"
  end

  defp get_processor(broadway_name, key, index \\ 1) do
    :"#{broadway_name}.Processor_#{key}_#{index}"
  end

  defp get_batcher(broadway_name, key) do
    :"#{broadway_name}.Batcher_#{key}"
  end

  defp get_consumer(broadway_name, key, index \\ 1) do
    :"#{broadway_name}.Consumer_#{key}_#{index}"
  end

  defp get_n_producers(broadway_name) do
    Supervisor.count_children(:"#{broadway_name}.ProducerSupervisor").workers
  end

  defp get_n_processors(broadway_name) do
    Supervisor.count_children(:"#{broadway_name}.ProcessorSupervisor").workers
  end

  defp get_n_consumers(broadway_name, key) do
    Supervisor.count_children(:"#{broadway_name}.ConsumerSupervisor_#{key}").workers
  end

  defp async_push_messages(producer, list) do
    send(producer, {:"$gen_call", {self(), make_ref()}, {:push_messages, wrap_messages(list)}})
  end

  defp push_messages(producer, list) do
    Producer.push_messages(producer, wrap_messages(list))
  end

  defp wrap_messages(list) do
    Enum.map(list, fn data ->
      %Message{data: data, acknowledger: {Acker, %{id: data, test_pid: self()}}}
    end)
  end
end
