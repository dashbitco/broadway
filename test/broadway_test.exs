defmodule BroadwayTest do
  use ExUnit.Case

  import Integer
  import ExUnit.CaptureLog

  alias Broadway.{Message, BatchInfo, CallerAcknowledger}

  defmodule Acker do
    @behaviour Broadway.Acknowledger

    def ack(_ack_ref, successful, failed) do
      test_pid =
        case {successful, failed} do
          {[%Message{acknowledger: {_, _ack_ref, %{test_pid: pid}}} | _], _failed} ->
            pid

          {_successful, [%Message{acknowledger: {_, _ack_ref, %{test_pid: pid}}} | _]} ->
            pid
        end

      send(test_pid, {:ack, successful, failed})
    end
  end

  defmodule ManualProducer do
    use GenStage

    @behaviour Broadway.Producer

    def start_link(args, opts \\ []) do
      GenStage.start_link(__MODULE__, args, opts)
    end

    @impl true
    def init(%{test_pid: test_pid}) do
      name = Process.info(self())[:registered_name]
      send(test_pid, {:producer_initialized, name})
      {:producer, %{test_pid: test_pid}}
    end

    def init(_args) do
      {:producer, %{}}
    end

    @impl true
    def handle_demand(_demand, state) do
      {:noreply, [], state}
    end

    @impl true
    def handle_info({:push_messages_async, messages}, state) do
      {:noreply, messages, state}
    end

    @impl true
    def prepare_for_draining(%{test_pid: test_pid}) do
      message = wrap_message(:message_during_cancel, test_pid)
      send(self(), {:push_messages_async, [message]})

      message = wrap_message(:message_after_cancel, test_pid)
      Process.send_after(self(), {:push_messages_async, [message]}, 1)
      :ok
    end

    @impl true
    def prepare_for_draining(_state) do
      :ok
    end

    defp wrap_message(message, test_pid) do
      ack = {CallerAcknowledger, {test_pid, make_ref()}, :ok}
      %Message{data: message, acknowledger: ack}
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

    def handle_message(:default, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data})
      message
    end

    def handle_batch(batcher, messages, _, %{test_pid: test_pid}) do
      send(test_pid, {:batch_handled, batcher, messages})
      messages
    end
  end

  defmodule CustomHandlers do
    use Broadway

    def handle_message(_, message, %{handle_message: handler} = context) do
      handler.(message, context)
    end

    def handle_batch(batcher, messages, batch_info, %{handle_batch: handler} = context) do
      handler.(batcher, messages, batch_info, context)
    end
  end

  defmodule CustomHandlersWithoutHandleBatch do
    use Broadway

    def handle_message(_, message, %{handle_message: handler} = context) do
      handler.(message, context)
    end
  end

  defmodule Transformer do
    def transform(event, test_pid: test_pid) do
      if event == :kill_producer do
        raise "Error raised"
      end

      %Message{
        data: "#{event} transformed",
        acknowledger: {Acker, :ack_ref, %{id: event, test_pid: test_pid}}
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
               shutdown: :infinity
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
          default: [module: {ManualProducer, []}]
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
          default: [module: {ManualProducer, []}, stages: 3]
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
          default: [module: {ManualProducer, []}]
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
          default: [module: {ManualProducer, []}]
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
          default: [module: {ManualProducer, []}]
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
          default: [module: {ManualProducer, []}]
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
          default: [module: {ManualProducer, []}]
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
          default: [module: {ManualProducer, []}]
        ],
        processors: [default: []],
        batchers: [default: []]
      )

    Broadway.push_messages(pid, [
      %Message{data: 1, acknowledger: {__MODULE__, :ack_ref, 1}},
      %Message{data: 3, acknowledger: {__MODULE__, :ack_ref, 3}}
    ])

    assert_receive {:message_handled, 1}
    assert_receive {:message_handled, 3}
  end

  describe "processor" do
    setup do
      test_pid = self()

      handle_message = fn message, _ ->
        case message.data do
          :fail -> Message.failed(message, "Failed message")
          :raise -> raise "Error raised"
          :bad_return -> :oops
          :bad_batcher -> %{message | batcher: :unknown}
          :broken_link -> Task.async(fn -> raise "oops" end) |> Task.await()
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

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producers: [
            default: [module: {ManualProducer, []}]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      processor = get_processor(broadway_name, :default)
      Process.link(Process.whereis(processor))
      %{broadway: broadway, processor: processor}
    end

    test "successful messages are marked as :ok", %{broadway: broadway} do
      ref = Broadway.test_messages(broadway, [1, 2], batch_mode: :bulk)
      assert_receive {:batch_handled, [%{status: :ok}, %{status: :ok}]}
      assert_receive {:ack, ^ref, [%{status: :ok}, %{status: :ok}], []}
    end

    test "failed messages are marked as {:failed, reason}", %{broadway: broadway} do
      ref = Broadway.test_messages(broadway, [:fail])
      assert_receive {:ack, ^ref, _, [%{status: {:failed, "Failed message"}}]}
    end

    test "failed messages are not forwarded to the batcher", %{broadway: broadway} do
      ref = Broadway.test_messages(broadway, [1, :fail, :fail, 4], batch_mode: :bulk)

      assert_receive {:batch_handled, [%{data: 1}, %{data: 4}]}
      assert_receive {:ack, ^ref, [%{status: :ok}, %{status: :ok}], []}
      assert_receive {:ack, ^ref, _, [%{status: {:failed, "Failed message"}}]}
      assert_receive {:ack, ^ref, _, [%{status: {:failed, "Failed message"}}]}
    end

    test "messages are marked as {:failed, reason} when an error is raised while processing",
         %{broadway: broadway, processor: processor} do
      assert capture_log(fn ->
               ref = Broadway.test_messages(broadway, [1, :raise, 4], batch_mode: :bulk)

               assert_receive {:ack, ^ref, [],
                               [%{status: {:failed, "due to an unhandled error"}}]}

               assert_receive {:ack, ^ref, [%{data: 1}, %{data: 4}], []}
             end) =~ "[error] ** (RuntimeError) Error raised"

      refute_received {:EXIT, _, ^processor}
    end

    test "messages are marked as {:failed, reason} on bad return",
         %{broadway: broadway, processor: processor} do
      assert capture_log(fn ->
               ref = Broadway.test_messages(broadway, [1, :bad_return, 4], batch_mode: :bulk)

               assert_receive {:ack, ^ref, [],
                               [%{status: {:failed, "due to an unhandled error"}}]}

               assert_receive {:ack, ^ref, [%{data: 1}, %{data: 4}], []}
             end) =~
               "[error] ** (RuntimeError) expected a Broadway.Message from handle_message/3, got :oops"

      refute_received {:EXIT, _, ^processor}
    end

    test "messages are marked as {:failed, reason} on bad batcher",
         %{broadway: broadway, processor: processor} do
      assert capture_log(fn ->
               ref = Broadway.test_messages(broadway, [1, :bad_batcher, 4], batch_mode: :bulk)

               assert_receive {:ack, ^ref, [],
                               [%{status: {:failed, "due to an unhandled error"}}]}

               assert_receive {:ack, ^ref, [%{data: 1}, %{data: 4}], []}
             end) =~
               "[error] ** (RuntimeError) message was set to unknown batcher :unknown. The known batchers are [:default]"

      refute_received {:EXIT, _, ^processor}
    end

    test "processors do not crash on bad acknowledger",
         %{broadway: broadway, processor: processor} do
      [one, raise, four] = wrap_messages([1, :raise, 4])
      raise = %{raise | acknowledger: {Unknown, :ack_ref, :ok}}

      assert capture_log(fn ->
               Broadway.push_messages(broadway, [one, raise, four])
               assert_receive {:ack, _, [%{data: 1}, %{data: 4}], []}
             end) =~ "[error] ** (UndefinedFunctionError) function Unknown.ack/3 is undefined"

      refute_received {:EXIT, _, ^processor}
    end

    test "processors do not crash on broken link", %{broadway: broadway, processor: processor} do
      assert capture_log(fn ->
               ref = Broadway.test_messages(broadway, [1, :broken_link, 4], batch_mode: :bulk)

               assert_receive {:ack, ^ref, [], [%{status: {:failed, "due to an unhandled exit"}}]}

               assert_receive {:ack, ^ref, [%{data: 1}, %{data: 4}], []}
             end) =~ "** (RuntimeError) oops"

      refute_received {:EXIT, _, ^processor}
    end
  end

  describe "pipeline without batchers" do
    setup do
      handle_message = fn message, _ ->
        case message.data do
          :fail -> Message.failed(message, "Failed message")
          _ -> message
        end
      end

      context = %{
        handle_message: handle_message
      }

      broadway_name = new_unique_name()

      {:ok, broadway} =
        Broadway.start_link(CustomHandlersWithoutHandleBatch,
          name: broadway_name,
          context: context,
          producers: [
            default: [module: {ManualProducer, []}]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 2]]
        )

      producer = get_producer(broadway_name, :default)

      %{broadway_name: broadway_name, broadway: broadway, producer: producer}
    end

    test "no batcher supervisor is initialized", %{broadway_name: broadway_name} do
      assert Process.whereis(:"#{broadway_name}.Broadway.BatcherPartitionSupervisor") == nil
    end

    test "successful messages are marked as :ok", %{broadway: broadway} do
      ref = Broadway.test_messages(broadway, [1, 2])
      assert_receive {:ack, ^ref, [%{status: :ok}], []}
      assert_receive {:ack, ^ref, [%{status: :ok}], []}
    end

    test "failed messages are marked as {:failed, reason}", %{broadway: broadway} do
      ref = Broadway.test_messages(broadway, [:fail])
      assert_receive {:ack, ^ref, _, [%{status: {:failed, "Failed message"}}]}
    end

    test "shutting down broadway waits until all events are processed",
         %{broadway: broadway, producer: producer} do
      # We suspend the producer to make sure that it doesn't process the messages early on
      :sys.suspend(producer)
      async_push_messages(producer, [1, 2, 3, 4])
      Process.exit(broadway, :shutdown)
      :sys.resume(producer)

      assert_receive {:ack, _, [%{data: 1}], []}
      assert_receive {:ack, _, [%{data: 2}], []}
      assert_receive {:ack, _, [%{data: 3}], []}
      assert_receive {:ack, _, [%{data: 4}], []}
    end
  end

  describe "put_batcher" do
    setup do
      broadway_name = new_unique_name()
      test_pid = self()

      handle_message = fn message, _ ->
        if is_odd(message.data) do
          Message.put_batcher(message, :odd)
        else
          Message.put_batcher(message, :even)
        end
      end

      context = %{
        handle_message: handle_message,
        handle_batch: fn batcher, batch, batch_info, _ ->
          send(test_pid, {:batch_handled, batcher, batch, batch_info})
          batch
        end
      }

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producers: [
            default: [module: {ManualProducer, []}]
          ],
          processors: [default: []],
          batchers: [
            odd: [batch_size: 10, batch_timeout: 20],
            even: [batch_size: 5, batch_timeout: 20]
          ]
        )

      %{broadway: broadway}
    end

    test "generate batches based on :batch_size", %{broadway: broadway} do
      Broadway.test_messages(broadway, Enum.to_list(1..40), batch_mode: :bulk)

      assert_receive {:batch_handled, :odd, messages, %BatchInfo{batcher: :odd}}
                     when length(messages) == 10

      assert_receive {:batch_handled, :odd, messages, %BatchInfo{batcher: :odd}}
                     when length(messages) == 10

      assert_receive {:batch_handled, :even, messages, %BatchInfo{batcher: :even}}
                     when length(messages) == 5

      assert_receive {:batch_handled, :even, messages, %BatchInfo{batcher: :even}}
                     when length(messages) == 5

      assert_receive {:batch_handled, :even, messages, %BatchInfo{batcher: :even}}
                     when length(messages) == 5

      assert_receive {:batch_handled, :even, messages, %BatchInfo{batcher: :even}}
                     when length(messages) == 5

      refute_receive {:batch_handled, _, _}
    end

    test "generate batches with the remaining messages after :batch_timeout is reached",
         %{broadway: broadway} do
      Broadway.test_messages(broadway, [1, 2, 3, 4, 5])

      assert_receive {:batch_handled, :odd, messages, _} when length(messages) == 3
      assert_receive {:batch_handled, :even, messages, _} when length(messages) == 2
      refute_receive {:batch_handled, _, _, _}
    end
  end

  describe "put_batch_key" do
    setup do
      test_pid = self()
      broadway_name = new_unique_name()

      handle_message = fn message, _ ->
        if is_odd(message.data) do
          Message.put_batch_key(message, :odd)
        else
          Message.put_batch_key(message, :even)
        end
      end

      context = %{
        handle_message: handle_message,
        handle_batch: fn batcher, batch, batch_info, _ ->
          send(test_pid, {:batch_handled, batcher, batch_info})
          batch
        end
      }

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producers: [default: [module: {ManualProducer, []}]],
          processors: [default: []],
          batchers: [default: [batch_size: 2, batch_timeout: 20]]
        )

      %{broadway: broadway}
    end

    test "generate batches based on :batch_size", %{broadway: broadway} do
      ref = Broadway.test_messages(broadway, Enum.to_list(1..10), batch_mode: :bulk)

      assert_receive {:ack, ^ref, [%{data: 1, batch_key: :odd}, %{data: 3, batch_key: :odd}], []}

      assert_receive {:ack, ^ref, [%{data: 2, batch_key: :even}, %{data: 4, batch_key: :even}],
                      []}

      assert_receive {:batch_handled, :default, %BatchInfo{batch_key: :odd}}
      assert_receive {:batch_handled, :default, %BatchInfo{batch_key: :even}}
    end

    test "generate batches with the remaining messages after :batch_timeout is reached",
         %{broadway: broadway} do
      ref = Broadway.test_messages(broadway, [1, 2])

      assert_receive {:ack, ^ref, [%{data: 1, batch_key: :odd}], []}
      assert_receive {:ack, ^ref, [%{data: 2, batch_key: :even}], []}
    end
  end

  describe "put_batch_mode" do
    setup do
      test_pid = self()
      broadway_name = new_unique_name()

      handle_message = fn message, _ ->
        message
      end

      context = %{
        handle_message: handle_message,
        handle_batch: fn batcher, batch, batch_info, _ ->
          send(test_pid, {:batch_handled, batcher, batch_info})
          batch
        end
      }

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producers: [default: [module: {ManualProducer, []}]],
          processors: [default: []],
          batchers: [default: [batch_size: 2, batch_timeout: 20]]
        )

      %{broadway: broadway}
    end

    test "flush by default", %{broadway: broadway} do
      ref = Broadway.test_messages(broadway, [1, 2, 3, 4, 5])
      assert_receive {:batch_handled, :default, _}
      assert_receive {:ack, ^ref, [%{data: 1}], []}
      assert_receive {:batch_handled, :default, _}
      assert_receive {:ack, ^ref, [%{data: 2}], []}
      assert_receive {:batch_handled, :default, _}
      assert_receive {:ack, ^ref, [%{data: 3}], []}
      assert_receive {:batch_handled, :default, _}
      assert_receive {:ack, ^ref, [%{data: 4}], []}
      assert_receive {:batch_handled, :default, _}
      assert_receive {:ack, ^ref, [%{data: 5}], []}
    end

    test "bulk", %{broadway: broadway} do
      ref = Broadway.test_messages(broadway, [1, 2, 3, 4, 5], batch_mode: :bulk)
      assert_receive {:batch_handled, :default, _}
      assert_receive {:ack, ^ref, [%{data: 1}, %{data: 2}], []}
      assert_receive {:batch_handled, :default, _}
      assert_receive {:ack, ^ref, [%{data: 3}, %{data: 4}], []}
      assert_receive {:batch_handled, :default, _}
      assert_receive {:ack, ^ref, [%{data: 5}], []}
    end
  end

  describe "ack_immediately" do
    test "acks a single message" do
      broadway_name = new_unique_name()
      test_pid = self()

      handle_message = fn message, _ ->
        message = Message.ack_immediately(message)
        send(test_pid, :manually_acked)
        message
      end

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: %{handle_message: handle_message},
          producers: [
            default: [module: {ManualProducer, []}]
          ],
          processors: [default: []]
        )

      ref = Broadway.test_messages(broadway, [1])

      assert_receive {:ack, ^ref, [%Message{data: 1}], []}
      assert_receive :manually_acked
      refute_receive {:ack, ^ref, _successful, _failed}
    end

    test "acks multiple messages" do
      broadway_name = new_unique_name()
      test_pid = self()

      handle_message = fn message, _ ->
        Message.put_batcher(message, :default)
      end

      handle_batch = fn :default, batch, _batch_info, _ ->
        batch =
          Enum.map(batch, fn
            %Message{data: :ok} = message -> message
            %Message{data: :fail} = message -> Message.failed(message, :manually_failed)
          end)

        batch = Message.ack_immediately(batch)
        send(test_pid, :manually_acked)
        batch
      end

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: %{handle_message: handle_message, handle_batch: handle_batch},
          producers: [
            default: [module: {ManualProducer, []}]
          ],
          processors: [default: []],
          batchers: [default: [batch_size: 4, batch_timeout: 1000]]
        )

      ref = Broadway.test_messages(broadway, [:ok, :ok, :fail, :ok], batch_mode: :bulk)

      assert_receive {:ack, ^ref, [_, _, _], [_]}
      assert_receive :manually_acked
      refute_receive {:ack, ^ref, _successful, _failed}
    end
  end

  describe "configure_ack" do
    test "raises if the acknowledger doesn't implement the configure/3 callback" do
      broadway_name = new_unique_name()

      handle_message = fn message, _ ->
        Message.configure_ack(message, test_pid: self())
      end

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: %{handle_message: handle_message},
          producers: [
            default: [module: {ManualProducer, []}]
          ],
          processors: [default: []]
        )

      log =
        capture_log(fn ->
          Broadway.push_messages(broadway, [
            %Message{data: 1, acknowledger: {Acker, nil, %{test_pid: self()}}}
          ])

          assert_receive {:ack, _successful = [], [failed]}
          assert failed.status == {:failed, "due to an unhandled error"}
        end)

      assert log =~ "the configure/3 callback is not defined by acknowledger BroadwayTest.Acker"
    end

    test "configures the acknowledger" do
      broadway_name = new_unique_name()
      configure_options = [some_unique_term: make_ref()]

      handle_message = fn message, _ ->
        Message.configure_ack(message, configure_options)
      end

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: %{handle_message: handle_message},
          producers: [
            default: [module: {ManualProducer, []}]
          ],
          processors: [default: []]
        )

      ref = Broadway.test_messages(broadway, [1])

      assert_receive {:configure, ^ref, ^configure_options}
      assert_receive {:ack, ^ref, [success], _failed = []}
      assert success.data == 1
    end
  end

  describe "transformer" do
    setup tags do
      broadway_name = new_unique_name()

      {:ok, _pid} =
        Broadway.start_link(Forwarder,
          name: broadway_name,
          resubscribe_interval: 0,
          context: %{test_pid: self()},
          producers: [
            default: [
              module: {EventProducer, Map.get(tags, :events, [1, 2, 3])},
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
      assert_receive {:message_handled, "1 transformed"}
      assert_receive {:message_handled, "2 transformed"}
      assert_receive {:message_handled, "3 transformed"}
    end

    @tag events: [1, 2, :kill_producer, 4]
    test "restart the producer if the transformation raises an error", context do
      %{producer: producer} = context
      ref_producer = Process.monitor(producer)

      assert_receive {:message_handled, "1 transformed"}
      assert_receive {:message_handled, "2 transformed"}
      assert_receive {:DOWN, ^ref_producer, _, _, _}
      assert_receive {:message_handled, "1 transformed"}
      assert_receive {:message_handled, "2 transformed"}
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

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          resubscribe_interval: 0,
          context: context,
          producers: [
            default: [
              module: {ManualProducer, %{test_pid: self()}}
            ]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      producer = get_producer(broadway_name, :default)
      processor = get_processor(broadway_name, :default)
      batcher = get_batcher(broadway_name, :default)
      consumer = get_consumer(broadway_name, :default)

      %{
        broadway: broadway,
        producer: producer,
        processor: processor,
        batcher: batcher,
        consumer: consumer
      }
    end

    test "only the producer will be restarted",
         %{producer: producer, processor: processor, batcher: batcher, consumer: consumer} do
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

    test "processors resubscribe to the restarted producers and keep processing messages",
         %{broadway: broadway, producer: producer} do
      assert_receive {:producer_initialized, ^producer}

      Broadway.test_messages(broadway, [1])
      assert_receive {:message_handled, %{data: 1}}

      GenStage.stop(producer)
      assert_receive {:producer_initialized, ^producer}

      Broadway.test_messages(broadway, [2])
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

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producers: [
            default: [module: {ManualProducer, []}]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      producer = get_producer(broadway_name, :default)
      processor = get_processor(broadway_name, :default)
      batcher = get_batcher(broadway_name, :default)
      consumer = get_consumer(broadway_name, :default)

      %{
        broadway: broadway,
        producer: producer,
        processor: processor,
        batcher: batcher,
        consumer: consumer
      }
    end

    test "processor will be restarted in order to handle other messages",
         %{broadway: broadway, processor: processor} do
      Broadway.test_messages(broadway, [1])
      assert_receive {:message_handled, %{data: 1}}

      pid = Process.whereis(processor)
      ref = Process.monitor(processor)
      Broadway.test_messages(broadway, [:kill_processor])
      assert_receive {:DOWN, ^ref, _, _, _}

      Broadway.test_messages(broadway, [2])
      assert_receive {:message_handled, %{data: 2}}
      assert Process.whereis(processor) != pid
    end

    test "processor crashes cascade down (but not up)", context do
      %{
        broadway: broadway,
        producer: producer,
        processor: processor,
        batcher: batcher,
        consumer: consumer
      } = context

      ref_batcher = Process.monitor(batcher)
      ref_processor = Process.monitor(processor)
      ref_producer = Process.monitor(producer)
      ref_consumer = Process.monitor(consumer)

      Broadway.test_messages(broadway, [:kill_processor])
      assert_receive {:DOWN, ^ref_processor, _, _, _}
      assert_receive {:DOWN, ^ref_batcher, _, _, _}
      assert_receive {:DOWN, ^ref_consumer, _, _, _}
      refute_received {:DOWN, ^ref_producer, _, _, _}
    end

    test "only the messages in the crashing processor are lost", %{broadway: broadway} do
      Broadway.test_messages(broadway, [1, 2, :kill_processor, 3, 4, 5], batch_mode: :bulk)

      assert_receive {:message_handled, %{data: 1}}
      assert_receive {:message_handled, %{data: 2}}

      refute_receive {:message_handled, %{data: :kill_processor}}
      refute_receive {:message_handled, %{data: 3}}

      assert_receive {:message_handled, %{data: 4}}
      assert_receive {:message_handled, %{data: 5}}
    end

    test "batches are created normally (without the lost messages)", %{broadway: broadway} do
      Broadway.test_messages(broadway, [1, 2, :kill_processor, 3, 4, 5], batch_mode: :bulk)

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
      broadway_name = new_unique_name()

      handle_batch = fn _, messages, _, _ ->
        pid = Process.whereis(get_batcher(broadway_name, :default))

        if Enum.any?(messages, fn msg -> msg.data == :kill_batcher end) do
          Process.exit(pid, :kill)
        end

        send(test_pid, {:batch_handled, messages, pid})
        messages
      end

      context = %{
        handle_batch: handle_batch,
        handle_message: fn message, _ -> message end
      }

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producers: [
            default: [module: {ManualProducer, []}]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      producer = get_producer(broadway_name, :default)
      processor = get_processor(broadway_name, :default)
      batcher = get_batcher(broadway_name, :default)
      consumer = get_consumer(broadway_name, :default)

      %{
        broadway: broadway,
        producer: producer,
        processor: processor,
        batcher: batcher,
        consumer: consumer
      }
    end

    test "batcher will be restarted in order to handle other messages", %{broadway: broadway} do
      Broadway.test_messages(broadway, [1, 2], batch_mode: :bulk)
      assert_receive {:batch_handled, _, batcher1} when is_pid(batcher1)
      ref = Process.monitor(batcher1)

      Broadway.test_messages(broadway, [:kill_batcher, 3], batch_mode: :bulk)
      assert_receive {:batch_handled, _, ^batcher1}
      assert_receive {:DOWN, ^ref, _, _, _}

      Broadway.test_messages(broadway, [4, 5], batch_mode: :bulk)
      assert_receive {:batch_handled, _, batcher2} when is_pid(batcher2)
      assert batcher1 != batcher2
    end

    test "batcher crashes cascade down (but not up)", context do
      %{
        broadway: broadway,
        producer: producer,
        processor: processor,
        batcher: batcher,
        consumer: consumer
      } = context

      ref_batcher = Process.monitor(batcher)
      ref_processor = Process.monitor(processor)
      ref_producer = Process.monitor(producer)
      ref_consumer = Process.monitor(consumer)

      Broadway.test_messages(broadway, [:kill_batcher, 3])
      assert_receive {:DOWN, ^ref_batcher, _, _, _}
      assert_receive {:DOWN, ^ref_consumer, _, _, _}
      refute_received {:DOWN, ^ref_producer, _, _, _}
      refute_received {:DOWN, ^ref_processor, _, _, _}
    end

    test "only the messages in the crashing batcher are lost", %{broadway: broadway} do
      ref =
        Broadway.test_messages(broadway, [1, 2, :kill_batcher, 3, 4, 5, 6, 7], batch_mode: :bulk)

      assert_receive {:ack, ^ref, successful, _failed}
      values = Enum.map(successful, & &1.data)
      assert values == [1, 2]

      assert_receive {:ack, ^ref, successful, _failed}
      values = Enum.map(successful, & &1.data)
      assert values == [:kill_batcher, 3]

      assert_receive {:ack, ^ref, successful, _failed}
      values = Enum.map(successful, & &1.data)
      assert values == [6, 7]

      refute_receive {:ack, _, _successful, _failed}
    end
  end

  describe "batcher" do
    setup do
      test_pid = self()

      handle_batch = fn _, batch, _, _ ->
        send(test_pid, {:batch_handled, batch})

        Enum.map(batch, fn message ->
          case message.data do
            :fail -> Message.failed(message, "Failed message")
            :raise -> raise "Error raised"
            :bad_return -> :oops
            :broken_link -> Task.async(fn -> raise "oops" end) |> Task.await()
            _ -> message
          end
        end)
      end

      context = %{
        handle_batch: handle_batch,
        handle_message: fn message, _ -> message end
      }

      broadway_name = new_unique_name()

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producers: [
            default: [module: {ManualProducer, []}]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 4]],
          batchers: [default: [batch_size: 4]]
        )

      consumer = get_consumer(broadway_name, :default)
      Process.link(Process.whereis(consumer))
      %{broadway: broadway, consumer: consumer}
    end

    test "messages are grouped by ack_ref + status (successful or failed) before sent for acknowledgement",
         %{broadway: broadway} do
      Broadway.push_messages(broadway, [
        %Message{data: 1, acknowledger: {CallerAcknowledger, {self(), :ack_ref_1}, :ok}},
        %Message{data: :fail, acknowledger: {CallerAcknowledger, {self(), :ack_ref_2}, :ok}},
        %Message{data: :fail, acknowledger: {CallerAcknowledger, {self(), :ack_ref_1}, :ok}},
        %Message{data: 4, acknowledger: {CallerAcknowledger, {self(), :ack_ref_2}, :ok}}
      ])

      assert_receive {:ack, :ack_ref_1, [%{data: 1}], [%{data: :fail}]}
      assert_receive {:ack, :ack_ref_2, [%{data: 4}], [%{data: :fail}]}
    end

    test "all messages in the batch are marked as {:failed, reason} when an error is raised",
         %{broadway: broadway, consumer: consumer} do
      assert capture_log(fn ->
               ref = Broadway.test_messages(broadway, [1, 2, :raise, 3], batch_mode: :bulk)

               assert_receive {:ack, ^ref, [],
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
         %{broadway: broadway, consumer: consumer} do
      assert capture_log(fn ->
               ref = Broadway.test_messages(broadway, [1, 2, :bad_return, 3], batch_mode: :bulk)

               assert_receive {:ack, ^ref, [],
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
         %{broadway: broadway, consumer: consumer} do
      [one, two, raise, three] = wrap_messages([1, 2, :raise, 3])
      raise = %{raise | acknowledger: {Unknown, :ack_ref, :ok}}

      assert capture_log(fn ->
               Broadway.push_messages(broadway, [one, two, raise, three])
               ref = Broadway.test_messages(broadway, [1, 2, 3, 4], batch_mode: :bulk)
               assert_receive {:ack, ^ref, [%{data: 1}, %{data: 2}, %{data: 3}, %{data: 4}], []}
             end) =~ "[error] ** (UndefinedFunctionError) function Unknown.ack/3 is undefined"

      refute_received {:EXIT, _, ^consumer}
    end

    test "consumers do not crash on broken link",
         %{broadway: broadway, consumer: consumer} do
      assert capture_log(fn ->
               ref = Broadway.test_messages(broadway, [1, 2, :broken_link, 3], batch_mode: :bulk)

               assert_receive {:ack, ^ref, [],
                               [
                                 %{data: 1, status: {:failed, "due to an unhandled exit"}},
                                 %{data: 2, status: {:failed, "due to an unhandled exit"}},
                                 %{
                                   data: :broken_link,
                                   status: {:failed, "due to an unhandled exit"}
                                 },
                                 %{data: 3, status: {:failed, "due to an unhandled exit"}}
                               ]}
             end) =~ "** (RuntimeError) oops"

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

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          producers: [
            default: [module: {ManualProducer, %{test_pid: self()}}]
          ],
          processors: [default: [stages: 1, min_demand: 1, max_demand: 4]],
          batchers: [default: [batch_size: 4]],
          context: context,
          shutdown: Map.get(tags, :shutdown, 5000)
        )

      producer = get_producer(broadway_name, :default)
      %{broadway: broadway, producer: producer}
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
      assert_receive {:ack, _, [%{data: 1}, %{data: 2}, %{data: 3}, %{data: 4}], []}
    end

    test "shutting down broadway waits until all events are processed even on incomplete batches",
         %{broadway: broadway} do
      ref = Broadway.test_messages(broadway, [1, 2], batch_mode: :bulk)
      Process.exit(broadway, :shutdown)
      assert_receive {:ack, ^ref, [%{data: 1}, %{data: 2}], []}
    end

    test "shutting down broadway cancels producers and waits for messages sent during cancellation",
         %{broadway: broadway} do
      Process.exit(broadway, :shutdown)

      assert_receive {:ack, _, [%{data: :message_during_cancel}], []}
      refute_receive {:ack, _, [%{data: :message_after_cancel}], []}
      assert_receive {:EXIT, ^broadway, :shutdown}
    end

    @tag shutdown: 1
    test "shutting down broadway respects shutdown value", %{broadway: broadway} do
      Broadway.test_messages(broadway, [:sleep, 1, 2, 3])
      Process.exit(broadway, :shutdown)
      assert_receive {:EXIT, ^broadway, :shutdown}
    end
  end

  defp new_unique_name() do
    :"Elixir.Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp get_producer(broadway_name, key, index \\ 1) do
    :"#{broadway_name}.Broadway.Producer_#{key}_#{index}"
  end

  defp get_processor(broadway_name, key, index \\ 1) do
    :"#{broadway_name}.Broadway.Processor_#{key}_#{index}"
  end

  defp get_batcher(broadway_name, key) do
    :"#{broadway_name}.Broadway.Batcher_#{key}"
  end

  defp get_consumer(broadway_name, key, index \\ 1) do
    :"#{broadway_name}.Broadway.Consumer_#{key}_#{index}"
  end

  defp get_n_producers(broadway_name) do
    Supervisor.count_children(:"#{broadway_name}.Broadway.ProducerSupervisor").workers
  end

  defp get_n_processors(broadway_name) do
    Supervisor.count_children(:"#{broadway_name}.Broadway.ProcessorSupervisor").workers
  end

  defp get_n_consumers(broadway_name, key) do
    Supervisor.count_children(:"#{broadway_name}.Broadway.ConsumerSupervisor_#{key}").workers
  end

  defp async_push_messages(producer, list) do
    send(producer, {:"$gen_call", {self(), make_ref()}, {:push_messages, wrap_messages(list)}})
  end

  defp wrap_messages(list) do
    ack = {CallerAcknowledger, {self(), make_ref()}, :ok}
    Enum.map(list, &%Message{data: &1, acknowledger: ack})
  end
end
