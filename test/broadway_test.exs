defmodule BroadwayTest do
  use ExUnit.Case

  import Integer
  import ExUnit.CaptureLog

  alias Broadway.{Message, BatchInfo, CallerAcknowledger}

  defmodule ManualProducer do
    @behaviour Broadway.Producer
    use GenStage

    @impl true
    def init(%{test_pid: test_pid}) do
      name = Process.info(self())[:registered_name]
      send(test_pid, {:producer_initialized, name})
      {:producer, %{test_pid: test_pid}, []}
    end

    def init(_args) do
      {:producer, %{}}
    end

    @impl true
    def handle_demand(demand, state) do
      if test_pid = state[:test_pid] do
        send(test_pid, {:handle_demand_called, demand, System.monotonic_time()})
      end

      {:noreply, [], state}
    end

    @impl true
    def handle_info({:push_messages, messages}, state) do
      {:noreply, messages, state}
    end

    @impl true
    def handle_cast({:push_messages, messages}, state) do
      {:noreply, messages, state}
    end

    @impl true
    def handle_call({:push_messages, messages}, _from, state) do
      {:reply, :reply, messages, state}
    end

    @impl true
    def prepare_for_draining(%{test_pid: test_pid} = state) do
      message = wrap_message(:message_during_cancel, test_pid)
      send(self(), {:push_messages, [message]})

      message = wrap_message(:message_after_cancel, test_pid)
      Process.send_after(self(), {:push_messages, [message]}, 1)
      {:noreply, [], state}
    end

    @impl true
    def prepare_for_draining(state) do
      {:noreply, [], state}
    end

    defp wrap_message(message, test_pid) do
      ack = {CallerAcknowledger, {test_pid, make_ref()}, :ok}
      %Message{data: message, acknowledger: ack}
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

  defmodule CustomHandlersWithHandleFailed do
    use Broadway

    def handle_message(_, message, %{handle_message: handler} = context) do
      handler.(message, context)
    end

    def handle_batch(batcher, messages, batch_info, %{handle_batch: handler} = context) do
      handler.(batcher, messages, batch_info, context)
    end

    def handle_failed(messages, %{handle_failed: handler} = context) do
      handler.(messages, context)
    end
  end

  defmodule CustomHandlersWithPreparedMessages do
    use Broadway

    def prepare_messages(messages, %{prepare_messages: prepare} = context) do
      prepare.(messages, context)
    end

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
        acknowledger: {Broadway.CallerAcknowledger, {test_pid, :ref}, :unused}
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

    test "invalid nested configuration options" do
      opts = [
        name: MyBroadway,
        producer: [module: {ManualProducer, []}],
        processors: [default: []],
        batchers: [
          sqs: [concurrency: 2, batch_sizy: 10]
        ]
      ]

      message = """
      invalid configuration given to Broadway.start_link/2 for key [:batchers, :sqs], \
      unknown options [:batch_sizy], valid options are: \
      [:concurrency, :batch_size, :batch_timeout, :partition_by, :spawn_opt, :hibernate_after]\
      """

      assert_raise ArgumentError, message, fn ->
        Broadway.start_link(Forwarder, opts)
      end
    end

    test "default number of producers is 1" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        context: %{test_pid: self()},
        producer: [module: {ManualProducer, []}],
        processors: [default: []],
        batchers: [default: []]
      )

      assert get_n_producers(broadway) == 1
      assert length(Broadway.producer_names(broadway)) == 1
    end

    test "set number of producers", %{test: test} do
      broadway = new_unique_name()
      self = self()

      :telemetry.attach(
        "#{test}",
        [:broadway, :supervisor, :init],
        fn name, measurements, metadata, _ ->
          send(self, {:telemetry_event, name, measurements, metadata})
        end,
        nil
      )

      Broadway.start_link(Forwarder,
        name: broadway,
        context: %{test_pid: self()},
        producer: [module: {ManualProducer, []}, concurrency: 3],
        processors: [default: []],
        batchers: [default: []]
      )

      assert get_n_producers(broadway) == 3
      assert length(Broadway.producer_names(broadway)) == 3

      assert_receive {:telemetry_event, [:broadway, :supervisor, :init], %{},
                      %{config: config, supervisor_pid: supervisor_pid}}
                     when is_pid(supervisor_pid)

      Enum.each(
        [
          :name,
          :producer,
          :processors
        ],
        fn key ->
          assert Keyword.has_key?(config, key)
        end
      )
    end

    test "default number of processors is schedulers_online * 2" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        producer: [module: {ManualProducer, []}],
        processors: [default: []],
        batchers: [default: []]
      )

      assert get_n_processors(broadway) == System.schedulers_online() * 2
    end

    test "set number of processors" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        producer: [module: {ManualProducer, []}],
        processors: [default: [concurrency: 13]],
        batchers: [default: []]
      )

      assert get_n_processors(broadway) == 13
    end

    test "default number of consumers is 1 per batcher" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        context: %{test_pid: self()},
        producer: [module: {ManualProducer, []}],
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
        producer: [module: {ManualProducer, []}],
        processors: [default: []],
        batchers: [
          p1: [concurrency: 2],
          p2: [concurrency: 3]
        ]
      )

      assert get_n_consumers(broadway, :p1) == 2
      assert get_n_consumers(broadway, :p2) == 3
    end

    test "default context is :context_not_set" do
      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        producer: [module: {ManualProducer, []}],
        processors: [default: []],
        batchers: [default: []]
      )

      pid = get_processor(broadway, :default)
      assert :sys.get_state(pid).state.context == :context_not_set
    end

    test "invokes prepare_for_start callback on producers" do
      defmodule PrepareProducer do
        @behaviour Broadway.Producer

        def prepare_for_start(Forwarder, opts) do
          name = opts[:name]

          child_spec = %{
            id: Agent,
            start: {Agent, :start_link, [fn -> %{} end, [name: Module.concat(name, "Agent")]]}
          }

          opts = put_in(opts[:producer][:module], {ManualProducer, []})
          {[child_spec], opts}
        end
      end

      broadway = new_unique_name()

      Broadway.start_link(Forwarder,
        name: broadway,
        producer: [module: {PrepareProducer, []}],
        processors: [default: []],
        batchers: [default: [batch_size: 2]],
        context: %{test_pid: self()}
      )

      # This is a message handled by ManualProducer,
      # meaning the prepare_for_start rewrite worked.
      send(
        get_producer(broadway),
        {:push_messages,
         [
           %Message{data: 1, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
           %Message{data: 3, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}}
         ]}
      )

      assert_receive {:message_handled, 1}
      assert_receive {:message_handled, 3}
      assert_receive {:batch_handled, :default, [%{data: 1}, %{data: 3}]}
      assert_receive {:ack, :ref, [_, _], []}

      # Now show that the agent was started
      assert Agent.get(Module.concat(broadway, "Agent"), & &1) == %{}

      # An that is a child of the main supervisor
      assert [_, _, _, _, _, {Agent, _, _, _}] =
               Supervisor.which_children(Module.concat(broadway, "Broadway.Supervisor"))
    end

    test "the return value of the prepare_for_start callback is validated" do
      defmodule BadPrepareProducer do
        @behaviour Broadway.Producer

        def prepare_for_start(Forwarder, opts) do
          opts[:context][:prepare_for_start_return_value]
        end
      end

      broadway = new_unique_name()

      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{} = error, _stacktrace}} =
               Broadway.start_link(Forwarder,
                 name: broadway,
                 producer: [module: {BadPrepareProducer, []}],
                 processors: [default: []],
                 context: %{prepare_for_start_return_value: :bad_return_value}
               )

      assert Exception.message(error) ==
               "expected BroadwayTest.BadPrepareProducer.prepare_for_start/2 to " <>
                 "return {child_specs, options}, got: :bad_return_value"
    end

    test "prepare_for_start callback validates options again" do
      defmodule BadPrepareProducerOptions do
        @behaviour Broadway.Producer

        def prepare_for_start(Forwarder, opts) do
          opts[:context][:prepare_for_start_return_value]
        end
      end

      broadway = new_unique_name()

      Process.flag(:trap_exit, true)

      assert {:error, {%NimbleOptions.ValidationError{} = error, _stacktrace}} =
               Broadway.start_link(Forwarder,
                 name: broadway,
                 producer: [module: {BadPrepareProducerOptions, []}],
                 processors: [default: []],
                 context: %{prepare_for_start_return_value: {[], _bad_opts = []}}
               )

      assert Exception.message(error) == "required option :name not found, received options: []"
    end

    test "injects the :broadway option when the producer config is a kw list" do
      defmodule ProducerWithTopologyIndex do
        @behaviour Broadway.Producer

        def init(opts) do
          send(opts[:test_pid], {:init_called, opts})
          {:producer, opts}
        end

        def handle_demand(_demand, state) do
          {:noreply, [], state}
        end
      end

      name = new_unique_name()

      Broadway.start_link(Forwarder,
        name: name,
        producer: [module: {ProducerWithTopologyIndex, [test_pid: self()]}],
        processors: [default: []]
      )

      assert_receive {:init_called, opts}
      assert opts[:broadway][:name] == name
      assert opts[:broadway][:index] == 0
      assert length(opts[:broadway][:processors]) == 1

      # If the producer config is not a kw list, the index is not injected.

      Broadway.start_link(Forwarder,
        name: new_unique_name(),
        producer: [module: {ProducerWithTopologyIndex, %{test_pid: self()}}],
        processors: [default: []]
      )

      assert_receive {:init_called, map}
      refute Map.has_key?(map, :broadway)
    end

    test "supports spawn_opt" do
      # At a given level
      Broadway.start_link(Forwarder,
        name: broadway_name = new_unique_name(),
        producer: [module: {ManualProducer, []}, spawn_opt: [priority: :high]],
        processors: [default: []]
      )

      assert named_priority(get_producer(broadway_name)) == {:priority, :high}
      assert named_priority(get_processor(broadway_name, :default)) == {:priority, :normal}

      # At the root
      Broadway.start_link(Forwarder,
        name: broadway_name = new_unique_name(),
        producer: [module: {ManualProducer, []}],
        processors: [default: []],
        spawn_opt: [priority: :high]
      )

      assert named_priority(get_producer(broadway_name)) == {:priority, :high}
      assert named_priority(get_processor(broadway_name, :default)) == {:priority, :high}
    end

    defp named_priority(name), do: Process.info(Process.whereis(name), :priority)
  end

  test "push_messages/2" do
    {:ok, _broadway} =
      Broadway.start_link(Forwarder,
        name: broadway_name = new_unique_name(),
        context: %{test_pid: self()},
        producer: [module: {ManualProducer, []}],
        processors: [default: []],
        batchers: [default: [batch_size: 2]]
      )

    Broadway.push_messages(broadway_name, [
      %Message{data: 1, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
      %Message{data: 3, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}}
    ])

    assert_receive {:message_handled, 1}
    assert_receive {:message_handled, 3}
    assert_receive {:batch_handled, :default, [%{data: 1}, %{data: 3}]}
    assert_receive {:ack, :ref, [_, _], []}
  end

  describe "test_messages/3" do
    setup do
      handle_message = fn message, _ ->
        message
      end

      context = %{
        handle_message: handle_message
      }

      broadway_name = new_unique_name()

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producer: [module: {ManualProducer, []}],
          processors: [default: [concurrency: 1, min_demand: 1, max_demand: 2]]
        )

      %{broadway: broadway_name}
    end

    test "metadata field is added to message if specified",
         %{broadway: broadway} do
      ref = Broadway.test_message(broadway, :message, metadata: %{field: :value})

      assert_receive {:ack, ^ref, [%Message{data: :message, metadata: %{field: :value}}], []}
    end

    test "metadata field in message defaults to %{} if not specified",
         %{broadway: broadway} do
      ref = Broadway.test_message(broadway, :message)

      assert_receive {:ack, ^ref, [%Message{data: :message, metadata: %{}}], []}
    end
  end

  describe "prepare_messages" do
    setup do
      prepare_messages = fn messages, _ ->
        messages
        |> Enum.reduce([], fn message, acc ->
          case message.data do
            :raise ->
              raise "Error raised in preparing message"

            :filter ->
              acc

            _ ->
              [message | acc]
          end
        end)
        |> Enum.reverse()
      end

      handle_message = fn
        message, _ -> message
      end

      context = %{
        prepare_messages: prepare_messages,
        handle_message: handle_message
      }

      broadway_name = new_unique_name()

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlersWithPreparedMessages,
          name: broadway_name,
          context: context,
          producer: [module: {ManualProducer, []}],
          processors: [default: []]
        )

      processor = get_processor(broadway_name, :default)
      Process.link(Process.whereis(processor))
      %{broadway: broadway_name, processor: processor}
    end

    test "raises if fewer messages are returned from prepare_messages", %{
      broadway: broadway,
      processor: processor
    } do
      assert capture_log(fn ->
               ref = Broadway.test_batch(broadway, [1, :filter, 3, 4], batch_mode: :flush)

               assert_receive {:ack, ^ref, [], [%{}, %{}, %{}, %{}]}
             end) =~
               "[error] ** (RuntimeError) expected all messages to be returned from prepared_messages/2"

      refute_received {:EXIT, _, ^processor}
    end

    test "all messages in the prepare_messages are marked as {kind, reason, stack} when an error is raised",
         %{broadway: broadway, processor: processor} do
      ref = Broadway.test_batch(broadway, [1, :raise, 3, 4], batch_mode: :flush)
      assert_receive {:ack, ^ref, [], [%{data: 1}, %{data: :raise}, %{data: 3}, %{data: 4}]}

      refute_received {:EXIT, _, ^processor}
    end

    test "all prepared messages are passed", %{broadway: broadway} do
      ref = Broadway.test_batch(broadway, [1, 2], batch_mode: :bulk)
      assert_receive {:ack, ^ref, [%{data: 1}, %{data: 2}], []}
    end
  end

  describe "processor" do
    setup do
      test_pid = self()

      handle_message = fn message, _ ->
        case message.data do
          :fail -> Message.failed(%{message | batcher: :unknown}, "Failed message")
          :raise -> raise "Error raised"
          :bad_return -> :oops
          :bad_batcher -> %{message | batcher: :unknown}
          :broken_link -> Task.async(fn -> raise "oops" end) |> Task.await()
          _ -> message
        end
      end

      handle_batch = fn _, batch, _, _ ->
        send(test_pid, {:batch_handled, batch})

        for message <- batch, message.data != :discard_in_batcher do
          if message.data == :fail_batcher do
            Message.failed(message, "Failed batcher")
          else
            message
          end
        end
      end

      context = %{
        handle_message: handle_message,
        handle_batch: handle_batch
      }

      broadway_name = new_unique_name()

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producer: [module: {ManualProducer, []}],
          processors: [default: [concurrency: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      processor = get_processor(broadway_name, :default)
      Process.link(Process.whereis(processor))
      %{broadway: broadway_name, processor: processor}
    end

    test "successful messages are marked as :ok", %{broadway: broadway} do
      ref = Broadway.test_batch(broadway, [1, 2], batch_mode: :bulk)
      assert_receive {:batch_handled, [%{status: :ok}, %{status: :ok}]}
      assert_receive {:ack, ^ref, [%{status: :ok}, %{status: :ok}], []}
    end

    test "failed messages are marked as {:failed, reason}", %{broadway: broadway} do
      ref = Broadway.test_message(broadway, :fail)
      assert_receive {:ack, ^ref, _, [%{status: {:failed, "Failed message"}}]}
    end

    test "failed messages are not forwarded to the batcher", %{broadway: broadway} do
      ref = Broadway.test_batch(broadway, [1, :fail, :fail, 4])

      assert_receive {:batch_handled, [%{data: 1}, %{data: 4}]}
      assert_receive {:ack, ^ref, [%{status: :ok}, %{status: :ok}], []}
      assert_receive {:ack, ^ref, _, [%{status: {:failed, "Failed message"}}]}
      assert_receive {:ack, ^ref, _, [%{status: {:failed, "Failed message"}}]}
    end

    test "messages are marked as {kind, reason, stack} when an error is raised while processing",
         %{broadway: broadway, processor: processor} do
      assert capture_log(fn ->
               ref = Broadway.test_batch(broadway, [1, :raise, 4])
               assert_receive {:ack, ^ref, [], [%{status: {:error, _, _}}]}
               assert_receive {:ack, ^ref, [%{data: 1}, %{data: 4}], []}
             end) =~ "[error] ** (RuntimeError) Error raised"

      refute_received {:EXIT, _, ^processor}
    end

    test "messages are marked as {kind, reason, stack} on bad return",
         %{broadway: broadway, processor: processor} do
      assert capture_log(fn ->
               ref = Broadway.test_batch(broadway, [1, :bad_return, 4])
               assert_receive {:ack, ^ref, [], [%{status: {:error, _, _}}]}
               assert_receive {:ack, ^ref, [%{data: 1}, %{data: 4}], []}
             end) =~
               "[error] ** (RuntimeError) expected a Broadway.Message from handle_message/3, got :oops"

      refute_received {:EXIT, _, ^processor}
    end

    test "messages are marked as {kind, reason, stack} on bad batcher",
         %{broadway: broadway, processor: processor} do
      assert capture_log(fn ->
               ref = Broadway.test_batch(broadway, [1, :bad_batcher, 4])
               assert_receive {:ack, ^ref, [], [%{status: {:error, _, _}}]}
               assert_receive {:ack, ^ref, [%{data: 1}, %{data: 4}], []}
             end) =~
               "[error] ** (RuntimeError) message was set to unknown batcher :unknown. The known batchers are [:default]"

      refute_received {:EXIT, _, ^processor}
    end

    test "messages are logged on incorrect batcher count",
         %{broadway: broadway, processor: processor} do
      assert capture_log(fn ->
               ref = Broadway.test_batch(broadway, [1, :discard_in_batcher, 4])

               assert_receive {:ack, ^ref, [%{data: 1}], []}
               assert_receive {:ack, ^ref, [%{data: 4}], []}
             end) =~
               "BroadwayTest.CustomHandlers.handle_batch/4 received 2 messages and returned only 1"

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
               ref = Broadway.test_batch(broadway, [1, :broken_link, 4])
               assert_receive {:ack, ^ref, [], [%{status: {:exit, _, _}}]}
               assert_receive {:ack, ^ref, [%{data: 1}, %{data: 4}], []}
             end) =~ "** (RuntimeError) oops"

      refute_received {:EXIT, _, ^processor}
    end

    test "telemetry", %{broadway: broadway, test: test} do
      self = self()

      :ok =
        :telemetry.attach_many(
          "#{test}",
          [
            [:broadway, :processor, :start],
            [:broadway, :processor, :stop],
            [:broadway, :batcher, :start],
            [:broadway, :batcher, :stop],
            [:broadway, :consumer, :start],
            [:broadway, :consumer, :stop],
            [:broadway, :processor, :message, :start],
            [:broadway, :processor, :message, :stop],
            [:broadway, :processor, :message, :exception]
          ],
          fn name, measurements, metadata, _ ->
            send(self, {:telemetry_event, name, measurements, metadata})
          end,
          nil
        )

      ref = Broadway.test_batch(broadway, [1, 2, :fail, :fail_batcher, :raise])

      assert_receive {:batch_handled, [%{data: 1, status: :ok}, %{data: 2, status: :ok}]}
      assert_receive {:batch_handled, [%{data: :fail_batcher, status: :ok}]}

      assert_receive {:ack, ^ref, [%{status: :ok}, %{status: :ok}], []}
      assert_receive {:ack, ^ref, [], [%{status: {:failed, "Failed message"}}]}
      assert_receive {:ack, ^ref, [], [%{status: {:failed, "Failed batcher"}}]}

      assert_receive {:telemetry_event, [:broadway, :processor, :start], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :message, :start], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :message, :stop], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :stop], %{}, metadata}
      assert [] = metadata.failed_messages

      assert_receive {:telemetry_event, [:broadway, :processor, :start], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :message, :start], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :message, :stop], %{}, %{}}

      assert_receive {:telemetry_event, [:broadway, :processor, :stop], %{},
                      %{failed_messages: []}}

      assert [] = metadata.failed_messages

      assert_receive {:telemetry_event, [:broadway, :processor, :start], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :message, :start], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :message, :stop], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :stop], %{}, metadata}
      assert [%{data: :fail}] = metadata.failed_messages

      assert_receive {:telemetry_event, [:broadway, :processor, :start], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :message, :start], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :message, :stop], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :stop], %{}, metadata}
      assert [] = metadata.failed_messages

      assert_receive {:telemetry_event, [:broadway, :batcher, :start], %{}, %{messages: [_]}}
      assert_receive {:telemetry_event, [:broadway, :batcher, :stop], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :batcher, :start], %{}, %{messages: [_]}}
      assert_receive {:telemetry_event, [:broadway, :batcher, :stop], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :batcher, :start], %{}, %{messages: [_]}}
      assert_receive {:telemetry_event, [:broadway, :batcher, :stop], %{}, %{}}

      assert_receive {:telemetry_event, [:broadway, :consumer, :start], %{},
                      %{messages: [%{data: 1}, %{data: 2}], batch_info: %BatchInfo{}}}

      assert_receive {:telemetry_event, [:broadway, :consumer, :stop], %{},
                      %{failed_messages: [], batch_info: %BatchInfo{}}}

      assert_receive {:telemetry_event, [:broadway, :consumer, :start], %{},
                      %{messages: [%{data: :fail_batcher}], batch_info: %BatchInfo{}}}

      assert_receive {:telemetry_event, [:broadway, :consumer, :stop], %{},
                      %{failed_messages: [%{data: :fail_batcher}], batch_info: %BatchInfo{}}}

      assert_receive {:telemetry_event, [:broadway, :processor, :start], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :message, :start], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :message, :exception], %{}, %{}}
      assert_receive {:telemetry_event, [:broadway, :processor, :stop], %{}, %{}}
      assert_receive {:ack, ^ref, [], [%{status: {:error, _, _}}]}
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

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlersWithoutHandleBatch,
          name: broadway_name,
          context: context,
          producer: [module: {ManualProducer, []}],
          processors: [default: [concurrency: 1, min_demand: 1, max_demand: 2]]
        )

      producer = get_producer(broadway_name)
      %{broadway: broadway_name, producer: producer}
    end

    test "no batcher supervisor is initialized", %{broadway: broadway} do
      assert Process.whereis(:"#{broadway}.Broadway.BatcherPartitionSupervisor") == nil
    end

    test "successful messages are marked as :ok", %{broadway: broadway} do
      ref = Broadway.test_batch(broadway, [1, 2], batch_mode: :flush)
      assert_receive {:ack, ^ref, [%{status: :ok}], []}
      assert_receive {:ack, ^ref, [%{status: :ok}], []}
    end

    test "failed messages are marked as {:failed, reason}", %{broadway: broadway} do
      ref = Broadway.test_message(broadway, :fail)
      assert_receive {:ack, ^ref, _, [%{status: {:failed, "Failed message"}}]}
    end

    test "shutting down broadway waits until all events are processed",
         %{broadway: broadway, producer: producer} do
      # We suspend the producer to make sure that it doesn't process the messages early on
      :sys.suspend(producer)
      async_push_messages(producer, [1, 2, 3, 4])
      Process.exit(Process.whereis(broadway), :shutdown)
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

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producer: [module: {ManualProducer, []}],
          processors: [default: []],
          batchers: [
            odd: [batch_size: 10, batch_timeout: 20],
            even: [batch_size: 5, batch_timeout: 20]
          ]
        )

      %{broadway: broadway_name}
    end

    test "generate batches based on :batch_size", %{broadway: broadway} do
      Broadway.test_batch(broadway, Enum.to_list(1..40))

      assert_receive {:batch_handled, :odd, messages, %BatchInfo{batcher: :odd, size: 10}}
                     when length(messages) == 10

      assert_receive {:batch_handled, :odd, messages, %BatchInfo{batcher: :odd, size: 10}}
                     when length(messages) == 10

      assert_receive {:batch_handled, :even, messages, %BatchInfo{batcher: :even, size: 5}}
                     when length(messages) == 5

      assert_receive {:batch_handled, :even, messages, %BatchInfo{batcher: :even, size: 5}}
                     when length(messages) == 5

      assert_receive {:batch_handled, :even, messages, %BatchInfo{batcher: :even, size: 5}}
                     when length(messages) == 5

      assert_receive {:batch_handled, :even, messages, %BatchInfo{batcher: :even, size: 5}}
                     when length(messages) == 5

      refute_received {:batch_handled, _, _}
    end

    test "generate batches with the remaining messages after :batch_timeout is reached",
         %{broadway: broadway} do
      Broadway.test_batch(broadway, [1, 2, 3, 4, 5], batch_mode: :flush)

      assert_receive {:batch_handled, :odd, messages, _} when length(messages) == 3
      assert_receive {:batch_handled, :even, messages, _} when length(messages) == 2
      refute_received {:batch_handled, _, _, _}
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

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producer: [module: {ManualProducer, []}],
          processors: [default: []],
          batchers: [default: [batch_size: 2, batch_timeout: 20]]
        )

      %{broadway: broadway_name}
    end

    test "generate batches based on :batch_size", %{broadway: broadway} do
      ref = Broadway.test_batch(broadway, Enum.to_list(1..10))

      assert_receive {:ack, ^ref, [%{data: 1, batch_key: :odd}, %{data: 3, batch_key: :odd}], []}

      assert_receive {:ack, ^ref, [%{data: 2, batch_key: :even}, %{data: 4, batch_key: :even}],
                      []}

      assert_receive {:batch_handled, :default, %BatchInfo{batch_key: :odd, size: 2}}
      assert_receive {:batch_handled, :default, %BatchInfo{batch_key: :even, size: 2}}
    end

    test "generate batches with the remaining messages after :batch_timeout is reached",
         %{broadway: broadway} do
      ref = Broadway.test_batch(broadway, [1, 2], batch_mode: :flush)

      assert_receive {:ack, ^ref, [%{data: 1, batch_key: :odd}], []}
      assert_receive {:ack, ^ref, [%{data: 2, batch_key: :even}], []}
    end
  end

  describe "partition_by" do
    @describetag processors_options: [], batchers_options: []

    setup tags do
      test_pid = self()
      broadway_name = new_unique_name()

      context = %{
        handle_message: fn message, _ ->
          message =
            if message.data >= 20 do
              Message.put_batch_key(message, :over_twenty)
            else
              message
            end

          Process.sleep(round(:random.uniform() * 20))
          send(test_pid, {:message_handled, message.data, self()})
          message
        end,
        handle_batch: fn _batcher, batch, _batch_info, _ ->
          Process.sleep(round(:random.uniform() * 20))
          send(test_pid, {:batch_handled, Enum.map(batch, & &1.data), self()})
          batch
        end
      }

      partition_by = fn msg -> msg.data end

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producer: [module: {ManualProducer, []}],
          processors: [
            default:
              [
                concurrency: 2
              ] ++ tags.processors_options
          ],
          batchers: [
            default:
              [
                concurrency: 2,
                batch_size: Map.get(tags, :batch_size, 2),
                batch_timeout: 80
              ] ++ tags.batchers_options
          ],
          partition_by: partition_by
        )

      %{broadway: broadway_name}
    end

    def shuffle_data(msg), do: if(msg.data <= 6, do: 0, else: 1)

    test "messages of the same partition are processed in order by the same processor",
         %{broadway: broadway} do
      Broadway.test_batch(broadway, Enum.to_list(1..8))

      assert_receive {:message_handled, 1, processor_1}
      assert_receive {:message_handled, data, ^processor_1}
      assert data == 3
      assert_receive {:message_handled, data, ^processor_1}
      assert data == 5
      assert_receive {:message_handled, data, ^processor_1}
      assert data == 7

      assert_receive {:message_handled, 2, processor_2}
      assert_receive {:message_handled, data, ^processor_2}
      assert data == 4
      assert_receive {:message_handled, data, ^processor_2}
      assert data == 6
      assert_receive {:message_handled, data, ^processor_2}
      assert data == 8

      assert processor_1 != processor_2
    end

    test "messages of the same partition are processed in order by the same batch processor",
         %{broadway: broadway} do
      Broadway.test_batch(broadway, Enum.to_list(1..12))

      assert_receive {:batch_handled, [1, 3], batch_processor_1}
      assert_receive {:batch_handled, data, ^batch_processor_1}
      assert data == [5, 7]
      assert_receive {:batch_handled, data, ^batch_processor_1}
      assert data == [9, 11]

      assert_receive {:batch_handled, [2, 4], batch_processor_2}
      assert_receive {:batch_handled, data, ^batch_processor_2}
      assert data == [6, 8]
      assert_receive {:batch_handled, data, ^batch_processor_2}
      assert data == [10, 12]

      assert batch_processor_1 != batch_processor_2
    end

    @tag batch_size: 4
    test "messages of the same partition are processed in order with batch key",
         %{broadway: broadway} do
      Broadway.test_batch(broadway, Enum.to_list(16..23))

      # Without the batch key, we would have two batches of 4 elements
      assert_receive {:batch_handled, [16, 18], _}
      assert_receive {:batch_handled, [17, 19], _}
      assert_receive {:batch_handled, [20, 22], _}
      assert_receive {:batch_handled, [21, 23], _}
    end

    @tag processors_options: [partition_by: &__MODULE__.shuffle_data/1],
         batchers_options: [partition_by: &__MODULE__.shuffle_data/1]
    test "messages with processors and batchers level partitioning",
         %{broadway: broadway} do
      Broadway.test_batch(broadway, Enum.to_list(1..12))

      assert_receive {:message_handled, 1, processor_1}
      assert_receive {:message_handled, data, ^processor_1}
      assert data == 2

      assert_receive {:message_handled, 7, processor_2}
      assert_receive {:message_handled, data, ^processor_2}
      assert data == 8

      assert_receive {:batch_handled, [1, 2], batch_processor_1}
      assert_receive {:batch_handled, data, ^batch_processor_1}
      assert data == [3, 4]

      assert_receive {:batch_handled, [7, 8], batch_processor_2}
      assert_receive {:batch_handled, data, ^batch_processor_2}
      assert data == [9, 10]

      assert processor_1 != processor_2
      assert batch_processor_1 != batch_processor_2
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

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producer: [module: {ManualProducer, []}],
          processors: [default: []],
          batchers: [default: [batch_size: 2, batch_timeout: 20]]
        )

      %{broadway: broadway_name}
    end

    test "flush by default", %{broadway: broadway} do
      ref = Broadway.test_batch(broadway, [1, 2, 3, 4, 5], batch_mode: :flush)
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
      ref = Broadway.test_batch(broadway, [1, 2, 3, 4, 5])
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

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: %{handle_message: handle_message},
          producer: [module: {ManualProducer, []}],
          processors: [default: []]
        )

      ref = Broadway.test_message(broadway_name, 1)

      assert_receive {:ack, ^ref, [%Message{data: 1}], []}
      assert_receive :manually_acked
      refute_received {:ack, ^ref, _successful, _failed}
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

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: %{handle_message: handle_message, handle_batch: handle_batch},
          producer: [module: {ManualProducer, []}],
          processors: [default: []],
          batchers: [default: [batch_size: 4, batch_timeout: 1000]]
        )

      ref = Broadway.test_batch(broadway_name, [:ok, :ok, :fail, :ok])

      assert_receive {:ack, ^ref, [_, _, _], [_]}
      assert_receive :manually_acked
      refute_received {:ack, ^ref, _successful, _failed}
    end
  end

  describe "configure_ack" do
    defmodule NonConfigurableAcker do
      @behaviour Broadway.Acknowledger

      def ack(ack_ref, successful, failed) do
        send(ack_ref, {:ack, successful, failed})
      end
    end

    test "raises if the acknowledger doesn't implement the configure/3 callback" do
      broadway_name = new_unique_name()

      handle_message = fn message, _ ->
        Message.configure_ack(message, test_pid: self())
      end

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: %{handle_message: handle_message},
          producer: [module: {ManualProducer, []}],
          processors: [default: []]
        )

      log =
        capture_log(fn ->
          Broadway.push_messages(broadway_name, [
            %Message{data: 1, acknowledger: {NonConfigurableAcker, self(), :unused}}
          ])

          assert_receive {:ack, _successful = [], [%{status: {:error, _, _}}]}
        end)

      assert log =~
               "the configure/3 callback is not defined by acknowledger BroadwayTest.NonConfigurableAcker"
    end

    test "configures the acknowledger" do
      broadway_name = new_unique_name()
      configure_options = [some_unique_term: make_ref()]

      handle_message = fn message, _ ->
        Message.configure_ack(message, configure_options)
      end

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: %{handle_message: handle_message},
          producer: [module: {ManualProducer, []}],
          processors: [default: []]
        )

      ref = Broadway.test_message(broadway_name, 1)

      assert_receive {:configure, ^ref, ^configure_options}
      assert_receive {:ack, ^ref, [success], _failed = []}
      assert success.data == 1
    end
  end

  describe "transformer" do
    setup do
      broadway_name = new_unique_name()

      {:ok, _pid} =
        Broadway.start_link(Forwarder,
          name: broadway_name,
          resubscribe_interval: 0,
          context: %{test_pid: self()},
          producer: [
            module: {ManualProducer, []},
            transformer: {Transformer, :transform, test_pid: self()}
          ],
          processors: [default: [concurrency: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      producer = get_producer(broadway_name)
      %{producer: producer}
    end

    test "transform all events on info", %{producer: producer} do
      send(producer, {:push_messages, [1, 2, 3]})
      assert_receive {:message_handled, "1 transformed"}
      assert_receive {:message_handled, "2 transformed"}
      assert_receive {:message_handled, "3 transformed"}
    end

    test "transform all events on call", %{producer: producer} do
      assert GenStage.call(producer, {:push_messages, [1, 2, 3]}) == :reply
      assert_receive {:message_handled, "1 transformed"}
      assert_receive {:message_handled, "2 transformed"}
      assert_receive {:message_handled, "3 transformed"}
    end

    test "transform all events on cast", %{producer: producer} do
      assert GenStage.cast(producer, {:push_messages, [1, 2, 3]}) == :ok
      assert_receive {:message_handled, "1 transformed"}
      assert_receive {:message_handled, "2 transformed"}
      assert_receive {:message_handled, "3 transformed"}
    end

    test "restart the producer if the transformation raises an error", %{producer: producer} do
      send(producer, {:push_messages, [1, 2]})
      send(producer, {:push_messages, [:kill_producer, 3]})
      ref_producer = Process.monitor(producer)

      assert_receive {:message_handled, "1 transformed"}
      assert_receive {:message_handled, "2 transformed"}
      assert_receive {:DOWN, ^ref_producer, _, _, _}
      refute_received {:message_handled, "3 transformed"}
    end
  end

  describe "handle_failed in the processor" do
    test "is called when a message is failed by the user" do
      broadway_name = new_unique_name()
      test_pid = self()

      handle_message = fn
        %{data: :fail} = message, _ -> Message.failed(message, :failed)
        message, _ -> message
      end

      handle_failed = fn [message], _context ->
        send(test_pid, {:handle_failed_called, message})
        [Message.update_data(message, fn _ -> :updated end)]
      end

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlersWithHandleFailed,
          name: broadway_name,
          context: %{handle_message: handle_message, handle_failed: handle_failed},
          producer: [module: {ManualProducer, []}],
          processors: [default: []]
        )

      ref = Broadway.test_batch(broadway_name, [1, :fail], batch_mode: :flush)

      assert_receive {:handle_failed_called, %Message{data: :fail}}
      assert_receive {:ack, ^ref, [successful], [failed]}
      assert successful.data == 1
      assert failed.data == :updated
    end

    test "is called when the processor raises when handling the message" do
      broadway_name = new_unique_name()
      test_pid = self()

      handle_message = fn
        %{data: :fail}, _ -> raise "forced failure"
        message, _ -> message
      end

      handle_failed = fn [message], _context ->
        send(test_pid, {:handle_failed_called, message})
        [Message.update_data(message, fn _ -> :updated end)]
      end

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlersWithHandleFailed,
          name: broadway_name,
          context: %{handle_message: handle_message, handle_failed: handle_failed},
          producer: [module: {ManualProducer, []}],
          processors: [default: []]
        )

      assert capture_log(fn ->
               ref = Broadway.test_batch(broadway_name, [1, :fail], batch_mode: :flush)

               assert_receive {:handle_failed_called, %Message{data: :fail}}
               assert_receive {:ack, ^ref, [successful], [failed]}
               assert successful.data == 1
               assert failed.data == :updated
             end) =~ "(RuntimeError) forced failure"
    end

    test "is wrapped in try/catch to contain failures" do
      broadway_name = new_unique_name()

      handle_message = fn
        %{data: :fail} = message, _ -> Message.failed(message, :some_reason)
      end

      handle_failed = fn [_message], _context ->
        raise "error in handle_failed"
      end

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlersWithHandleFailed,
          name: broadway_name,
          context: %{handle_message: handle_message, handle_failed: handle_failed},
          producer: [module: {ManualProducer, []}],
          processors: [default: []]
        )

      assert capture_log(fn ->
               ref = Broadway.test_message(broadway_name, :fail)
               assert_receive {:ack, ^ref, [], [%{data: :fail, status: {:failed, _}}]}
             end) =~ "(RuntimeError) error in handle_failed"
    end
  end

  describe "handle_failed in the batcher" do
    test "is called for messages that are failed by the user" do
      broadway_name = new_unique_name()
      test_pid = self()

      handle_batch = fn _, messages, _, _ ->
        Enum.map(messages, fn
          %{data: :fail} = message -> Message.failed(message, :failed)
          message -> message
        end)
      end

      handle_failed = fn messages, _context ->
        send(test_pid, {:handle_failed_called, messages})
        Enum.map(messages, &Message.update_data(&1, fn _ -> :updated end))
      end

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlersWithHandleFailed,
          name: broadway_name,
          context: %{
            handle_message: fn message, _context -> message end,
            handle_batch: handle_batch,
            handle_failed: handle_failed
          },
          producer: [module: {ManualProducer, []}],
          processors: [default: []],
          batchers: [default: []]
        )

      assert capture_log(fn ->
               ref = Broadway.test_batch(broadway_name, [1, :fail, :fail, 2], batch_mode: :flush)

               assert_receive {:handle_failed_called, messages}
               assert [%Message{data: :fail}, %Message{data: :fail}] = messages

               assert_receive {:ack, ^ref, successful, failed}
               assert [%{data: 1}, %{data: 2}] = successful
               assert [%{data: :updated}, %{data: :updated}] = failed
             end) == ""
    end

    test "is called for the whole batch if handle_batch crashes" do
      broadway_name = new_unique_name()
      test_pid = self()

      handle_batch = fn _, _, _, _ ->
        raise "handle_batch failed"
      end

      handle_failed = fn messages, _context ->
        send(test_pid, {:handle_failed_called, messages})
        Enum.map(messages, &Message.update_data(&1, fn _ -> :updated end))
      end

      assert capture_log(fn ->
               {:ok, _broadway} =
                 Broadway.start_link(CustomHandlersWithHandleFailed,
                   name: broadway_name,
                   context: %{
                     handle_message: fn message, _context -> message end,
                     handle_batch: handle_batch,
                     handle_failed: handle_failed
                   },
                   producer: [module: {ManualProducer, []}],
                   processors: [default: []],
                   batchers: [default: []]
                 )

               ref = Broadway.test_batch(broadway_name, [1, 2], batch_mode: :flush)

               assert_receive {:handle_failed_called, [_, _]}

               assert_receive {:ack, ^ref, _successful = [], failed}
               assert [%{data: :updated}, %{data: :updated}] = failed
             end) =~ "handle_batch failed"
    end

    test "is wrapped in try/catch to contain failures" do
      broadway_name = new_unique_name()

      handle_batch = fn _, messages, _, _ ->
        Enum.map(messages, &Message.failed(&1, :failed))
      end

      assert capture_log(fn ->
               {:ok, _broadway} =
                 Broadway.start_link(CustomHandlersWithHandleFailed,
                   name: broadway_name,
                   context: %{
                     handle_message: fn message, _context -> message end,
                     handle_batch: handle_batch,
                     handle_failed: fn _, _ -> raise "handle_failed failed" end
                   },
                   producer: [module: {ManualProducer, []}],
                   processors: [default: []],
                   batchers: [default: []]
                 )

               ref = Broadway.test_batch(broadway_name, [1, 2], batch_mode: :flush)

               assert_receive {:ack, ^ref, _successful = [], failed}
               assert [%{data: 1}, %{data: 2}] = failed
             end) =~ "(RuntimeError) handle_failed failed"
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

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          resubscribe_interval: 0,
          context: context,
          producer: [
            module: {ManualProducer, %{test_pid: self()}}
          ],
          processors: [default: [concurrency: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      producer = get_producer(broadway_name)
      processor = get_processor(broadway_name, :default)
      batcher = get_batcher(broadway_name, :default)
      consumer = get_batch_processor(broadway_name, :default)

      %{
        broadway: broadway_name,
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
      assert_receive {:producer_initialized, ^producer}
      refute_received {:DOWN, ^ref_processor, _, _, _}
      refute_received {:DOWN, ^ref_batcher, _, _, _}
      refute_received {:DOWN, ^ref_consumer, _, _, _}
    end

    test "processors resubscribe to the restarted producers and keep processing messages",
         %{broadway: broadway, producer: producer} do
      assert_receive {:producer_initialized, ^producer}

      Broadway.test_message(broadway, 1)
      assert_receive {:message_handled, %{data: 1}}

      GenStage.stop(producer)
      assert_receive {:producer_initialized, ^producer}

      Broadway.test_message(broadway, 2)
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

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producer: [module: {ManualProducer, []}],
          processors: [default: [concurrency: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      producer = get_producer(broadway_name)
      processor = get_processor(broadway_name, :default)
      batcher = get_batcher(broadway_name, :default)
      consumer = get_batch_processor(broadway_name, :default)

      %{
        broadway: broadway_name,
        producer: producer,
        processor: processor,
        batcher: batcher,
        consumer: consumer
      }
    end

    test "processor will be restarted in order to handle other messages",
         %{broadway: broadway, processor: processor} do
      Broadway.test_message(broadway, 1)
      assert_receive {:message_handled, %{data: 1}}

      pid = Process.whereis(processor)
      ref = Process.monitor(processor)
      Broadway.test_message(broadway, :kill_processor)
      assert_receive {:DOWN, ^ref, _, _, _}

      Broadway.test_message(broadway, 2)
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

      Broadway.test_message(broadway, :kill_processor)
      assert_receive {:DOWN, ^ref_processor, _, _, _}
      assert_receive {:DOWN, ^ref_batcher, _, _, _}
      assert_receive {:DOWN, ^ref_consumer, _, _, _}
      refute_received {:DOWN, ^ref_producer, _, _, _}
    end

    test "only the messages in the crashing processor are lost", %{broadway: broadway} do
      Broadway.test_batch(broadway, [1, 2, :kill_processor, 3, 4, 5])

      assert_receive {:message_handled, %{data: 1}}
      assert_receive {:message_handled, %{data: 2}}
      assert_receive {:message_handled, %{data: 4}}
      assert_receive {:message_handled, %{data: 5}}

      refute_received {:message_handled, %{data: :kill_processor}}
      refute_received {:message_handled, %{data: 3}}
    end

    test "batches are created normally (without the lost messages)", %{broadway: broadway} do
      Broadway.test_batch(broadway, [1, 2, :kill_processor, 3, 4, 5])

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

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producer: [module: {ManualProducer, []}],
          processors: [default: [concurrency: 1, min_demand: 1, max_demand: 2]],
          batchers: [default: [batch_size: 2]]
        )

      producer = get_producer(broadway_name)
      processor = get_processor(broadway_name, :default)
      batcher = get_batcher(broadway_name, :default)
      consumer = get_batch_processor(broadway_name, :default)

      %{
        broadway: broadway_name,
        producer: producer,
        processor: processor,
        batcher: batcher,
        consumer: consumer
      }
    end

    test "batcher will be restarted in order to handle other messages", %{broadway: broadway} do
      Broadway.test_batch(broadway, [1, 2])
      assert_receive {:batch_handled, _, batcher1} when is_pid(batcher1)
      ref = Process.monitor(batcher1)

      Broadway.test_batch(broadway, [:kill_batcher, 3])
      assert_receive {:batch_handled, _, ^batcher1}
      assert_receive {:DOWN, ^ref, _, _, _}

      Broadway.test_batch(broadway, [4, 5])
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

      Broadway.test_batch(broadway, [:kill_batcher, 3], batch_mode: :flush)
      assert_receive {:DOWN, ^ref_batcher, _, _, _}
      assert_receive {:DOWN, ^ref_consumer, _, _, _}
      refute_received {:DOWN, ^ref_producer, _, _, _}
      refute_received {:DOWN, ^ref_processor, _, _, _}
    end

    test "only the messages in the crashing batcher are lost", %{broadway: broadway} do
      ref = Broadway.test_batch(broadway, [1, 2, :kill_batcher, 3, 4, 5, 6, 7])

      assert_receive {:ack, ^ref, successful, _failed}
      values = Enum.map(successful, & &1.data)
      assert values == [1, 2]

      assert_receive {:ack, ^ref, successful, _failed}
      values = Enum.map(successful, & &1.data)
      assert values == [:kill_batcher, 3]

      assert_receive {:ack, ^ref, successful, _failed}
      values = Enum.map(successful, & &1.data)
      assert values == [6, 7]

      refute_received {:ack, _, _successful, _failed}
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

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          context: context,
          producer: [module: {ManualProducer, []}],
          processors: [default: [concurrency: 1, min_demand: 1, max_demand: 4]],
          batchers: [default: [batch_size: 4]]
        )

      consumer = get_batch_processor(broadway_name, :default)
      Process.link(Process.whereis(consumer))
      %{broadway: broadway_name, consumer: consumer}
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

    test "all messages in the batch are marked as {kind, reason, stack} when an error is raised",
         %{broadway: broadway, consumer: consumer} do
      assert capture_log(fn ->
               ref = Broadway.test_batch(broadway, [1, 2, :raise, 3])

               assert_receive {:ack, ^ref, [],
                               [
                                 %{data: 1, status: {:error, _, _}},
                                 %{data: 2, status: {:error, _, _}},
                                 %{data: :raise, status: {:error, _, _}},
                                 %{data: 3, status: {:error, _, _}}
                               ]}
             end) =~ "[error] ** (RuntimeError) Error raised"

      refute_received {:EXIT, _, ^consumer}
    end

    test "all messages in the batch are marked as {kind, reason, stack} on bad return",
         %{broadway: broadway, consumer: consumer} do
      assert capture_log(fn ->
               ref = Broadway.test_batch(broadway, [1, 2, :bad_return, 3])

               assert_receive {:ack, ^ref, [],
                               [
                                 %{data: 1, status: {:error, _, _}},
                                 %{data: 2, status: {:error, _, _}},
                                 %{data: :bad_return, status: {:error, _, _}},
                                 %{data: 3, status: {:error, _, _}}
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
               ref = Broadway.test_batch(broadway, [1, 2, 3, 4])
               assert_receive {:ack, ^ref, [%{data: 1}, %{data: 2}, %{data: 3}, %{data: 4}], []}
             end) =~ "[error] ** (UndefinedFunctionError) function Unknown.ack/3 is undefined"

      refute_received {:EXIT, _, ^consumer}
    end

    test "consumers do not crash on broken link",
         %{broadway: broadway, consumer: consumer} do
      assert capture_log(fn ->
               ref = Broadway.test_batch(broadway, [1, 2, :broken_link, 3])

               assert_receive {:ack, ^ref, [],
                               [
                                 %{data: 1, status: {:exit, _, _}},
                                 %{data: 2, status: {:exit, _, _}},
                                 %{data: :broken_link, status: {:exit, _, _}},
                                 %{data: 3, status: {:exit, _, _}}
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

      {:ok, _} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          producer: [module: {ManualProducer, %{test_pid: self()}}],
          processors: [default: [concurrency: 1, min_demand: 1, max_demand: 4]],
          batchers: [default: [batch_size: 4]],
          context: context,
          shutdown: Map.get(tags, :shutdown, 5000)
        )

      producer = get_producer(broadway_name)
      %{broadway: broadway_name, producer: producer}
    end

    test "killing the supervisor brings down the broadway GenServer",
         %{broadway: broadway} do
      pid = Process.whereis(broadway)
      %{supervisor_pid: supervisor_pid} = :sys.get_state(pid)
      Process.exit(supervisor_pid, :kill)
      assert_receive {:EXIT, ^pid, :killed}
    end

    test "shutting down broadway waits until the Broadway.Supervisor is down",
         %{broadway: broadway} do
      %{supervisor_pid: supervisor_pid} = :sys.get_state(broadway)
      pid = Process.whereis(broadway)
      Process.exit(pid, :shutdown)

      assert_receive {:EXIT, ^pid, :shutdown}
      refute Process.alive?(supervisor_pid)
    end

    test "shutting down broadway waits until all events are processed",
         %{broadway: broadway, producer: producer} do
      # We suspend the producer to make sure that it doesn't process the messages early on
      :sys.suspend(producer)
      async_push_messages(producer, [1, 2, 3, 4])
      Process.exit(Process.whereis(broadway), :shutdown)
      :sys.resume(producer)
      assert_receive {:ack, _, [%{data: 1}, %{data: 2}, %{data: 3}, %{data: 4}], []}
    end

    test "shutting down broadway waits until all events are processed even on incomplete batches",
         %{broadway: broadway} do
      ref = Broadway.test_batch(broadway, [1, 2])
      Process.exit(Process.whereis(broadway), :shutdown)
      assert_receive {:ack, ^ref, [%{data: 1}, %{data: 2}], []}
    end

    test "shutting down broadway cancels producers and waits for messages sent during cancellation",
         %{broadway: broadway} do
      pid = Process.whereis(broadway)
      Process.exit(pid, :shutdown)

      assert_receive {:ack, _, [%{data: :message_during_cancel}], []}
      assert_receive {:EXIT, ^pid, :shutdown}
      refute_received {:ack, _, [%{data: :message_after_cancel}], []}
    end

    @tag shutdown: 1
    test "shutting down broadway respects shutdown value", %{broadway: broadway} do
      Broadway.test_batch(broadway, [:sleep, 1, 2, 3], batch_mode: :flush)
      pid = Process.whereis(broadway)
      Process.exit(pid, :shutdown)
      assert_receive {:EXIT, ^pid, :shutdown}
    end
  end

  describe "rate limiting" do
    test "with an interval and exact allowed messages in that interval" do
      broadway_name = new_unique_name()

      {:ok, _broadway} =
        Broadway.start_link(Forwarder,
          name: broadway_name,
          producer: [
            module: {ManualProducer, []},
            concurrency: 1,
            rate_limiting: [allowed_messages: 1, interval: 10000]
          ],
          processors: [default: []],
          context: %{test_pid: self()}
        )

      producer = get_producer(broadway_name)

      send(
        producer,
        {:push_messages,
         [
           %Message{data: 1, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}}
         ]}
      )

      assert_receive {:ack, :ref, [_], []}

      # We "cheat" and manually tell the rate limiter to reset the limit.
      send(get_rate_limiter(broadway_name), :reset_limit)

      # Give a chance for other processes to run
      :erlang.yield()

      send(
        producer,
        {:push_messages,
         [
           %Message{data: 1, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}}
         ]}
      )

      assert_receive {:ack, :ref, [_], []}
    end

    test "with an interval and more than allowed messages in that interval" do
      broadway_name = new_unique_name()
      test_pid = self()

      handle_message = fn message, _ ->
        send(test_pid, {:handle_message_called, message, System.monotonic_time()})
        message
      end

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          producer: [
            module: {ManualProducer, []},
            concurrency: 2,
            rate_limiting: [allowed_messages: 2, interval: 50]
          ],
          processors: [default: []],
          context: %{handle_message: handle_message}
        )

      send(
        get_producer(broadway_name),
        {:push_messages,
         [
           %Message{data: 1, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
           %Message{data: 2, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
           %Message{data: 3, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
           %Message{data: 4, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
           %Message{data: 5, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}}
         ]}
      )

      assert_receive {:handle_message_called, %Message{data: 1}, timestamp1}
      assert_receive {:handle_message_called, %Message{data: 2}, _timestamp2}
      assert_receive {:handle_message_called, %Message{data: 3}, timestamp3}
      assert_receive {:handle_message_called, %Message{data: 4}, _timestamp4}
      assert_receive {:handle_message_called, %Message{data: 5}, timestamp5}

      assert_receive {:ack, :ref, [_, _], []}
      assert_receive {:ack, :ref, [_, _], []}
      assert_receive {:ack, :ref, [_], []}

      # To be safe, we're saying that these messages have to be at least 10ms apart
      # even if the interval is 50ms. This is because the time when the processor
      # receives the message is not the time when the producer produced it.
      assert System.convert_time_unit(timestamp3 - timestamp1, :native, :millisecond) >= 10
      assert System.convert_time_unit(timestamp5 - timestamp3, :native, :millisecond) >= 10
    end

    test "buffers demand and flushes it when the rate limiting is lifted" do
      broadway_name = new_unique_name()
      test_pid = self()

      handle_message = fn message, _ ->
        send(test_pid, {:handle_message_called, message, System.monotonic_time()})
        message
      end

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          producer: [
            module: {ManualProducer, %{test_pid: test_pid}},
            concurrency: 1,
            # We use a long rate limiting interval so that we can trigger the rate limiting
            # manually from this test.
            rate_limiting: [allowed_messages: 2, interval: 10000]
          ],
          processors: [default: [concurrency: 1, max_demand: 2]],
          context: %{handle_message: handle_message}
        )

      send(
        get_producer(broadway_name),
        {:push_messages,
         [
           %Message{data: 1, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
           %Message{data: 2, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
           %Message{data: 3, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}}
         ]}
      )

      # First, we assert that handle_demand is called and that two events are emitted.
      assert_receive {:handle_demand_called, _demand, demand_timestamp1}
      assert_receive {:handle_message_called, %Message{data: 1}, timestamp1}
      assert_receive {:handle_message_called, %Message{data: 2}, _timestamp2}

      assert demand_timestamp1 < timestamp1

      # Now, since the rate limiting interval is long, we can assert that
      # handle_demand is not called and that the third message is not emitted yet.
      refute_received {:handle_demand_called, _demand, _timestamp}
      refute_received {:handle_message_called, %Message{data: 3}, _timestamp3}

      # We "cheat" and manually tell the rate limiter to reset the limit.
      send(get_rate_limiter(broadway_name), :reset_limit)

      assert_receive {:handle_demand_called, _demand, demand_timestamp2}
      assert_receive {:handle_message_called, %Message{data: 3}, timestamp3}

      assert demand_timestamp2 < timestamp3
      assert demand_timestamp2 > timestamp1
    end

    test "works when stopping a pipeline and draining producers" do
      Process.flag(:trap_exit, true)

      broadway_name = new_unique_name()
      test_pid = self()

      handle_message = fn message, _ ->
        send(test_pid, {:handle_message_called, message, System.monotonic_time()})
        message
      end

      {:ok, broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          producer: [
            module: {ManualProducer, %{test_pid: test_pid}},
            concurrency: 1,
            # We use a long rate limiting interval so that we can trigger the rate limiting
            # manually from this test.
            rate_limiting: [allowed_messages: 2, interval: 10000]
          ],
          processors: [default: [concurrency: 1, max_demand: 2]],
          context: %{handle_message: handle_message}
        )

      # First, we use all the rate limiting we have available.
      send(
        get_producer(broadway_name),
        {:push_messages,
         [
           %Message{data: 1, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
           %Message{data: 2, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
           %Message{data: 3, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}}
         ]}
      )

      assert_receive {:handle_message_called, %Broadway.Message{data: 1}, timestamp1}
      assert_receive {:handle_message_called, %Broadway.Message{data: 2}, _timestamp2}

      # Then we stop the pipeline, triggering the :message_during_cancel message of
      # the ManualProducer.
      Process.exit(broadway, :shutdown)

      refute_received {:handle_message_called, %Broadway.Message{data: :message_during_cancel},
                       _timestamp}

      send(get_rate_limiter(broadway_name), :reset_limit)

      assert_receive {:handle_message_called, %Broadway.Message{data: 3}, timestamp3}

      assert_receive {:handle_message_called, %Broadway.Message{data: :message_during_cancel},
                      timestamp_cancel}

      assert timestamp_cancel > timestamp1
      assert timestamp3 < timestamp_cancel
    end

    test "getting and updating the rate limit at runtime" do
      broadway_name = new_unique_name()
      test_pid = self()

      handle_message = fn message, _ ->
        send(test_pid, {:handle_message_called, message, System.monotonic_time()})
        message
      end

      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name,
          producer: [
            module: {ManualProducer, []},
            concurrency: 2,
            rate_limiting: [allowed_messages: 1, interval: 5000]
          ],
          processors: [default: []],
          context: %{handle_message: handle_message}
        )

      assert Broadway.get_rate_limiting(broadway_name) ==
               {:ok, %{allowed_messages: 1, interval: 5000}}

      send(
        get_producer(broadway_name),
        {:push_messages,
         [
           %Message{data: 1, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
           %Message{data: 2, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
           %Message{data: 3, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}},
           %Message{data: 4, acknowledger: {CallerAcknowledger, {self(), :ref}, :unused}}
         ]}
      )

      assert_receive {:handle_message_called, %Message{data: 1}, _timestamp}

      refute_received {:handle_message_called, _message, _timestamp}

      assert :ok = Broadway.update_rate_limiting(broadway_name, allowed_messages: 3)

      assert Broadway.get_rate_limiting(broadway_name) ==
               {:ok, %{allowed_messages: 3, interval: 5000}}

      send(get_rate_limiter(broadway_name), :reset_limit)

      assert_receive {:handle_message_called, %Message{data: 2}, _timestamp}
      assert_receive {:handle_message_called, %Message{data: 3}, _timestamp}
      assert_receive {:handle_message_called, %Message{data: 4}, _timestamp}
    end

    test "invalid options" do
      {:ok, _broadway} =
        Broadway.start_link(CustomHandlers,
          name: broadway_name = new_unique_name(),
          producer: [module: {ManualProducer, []}],
          processors: [default: []]
        )

      assert_raise ArgumentError,
                   "invalid options, unknown options [:invalid_option], valid options are: [:allowed_messages, :interval]",
                   fn -> Broadway.update_rate_limiting(broadway_name, invalid_option: 3) end
    end
  end

  defp new_unique_name() do
    :"Elixir.Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp get_producer(broadway_name, index \\ 0) do
    :"#{broadway_name}.Broadway.Producer_#{index}"
  end

  defp get_processor(broadway_name, key, index \\ 0) do
    :"#{broadway_name}.Broadway.Processor_#{key}_#{index}"
  end

  defp get_batcher(broadway_name, key) do
    :"#{broadway_name}.Broadway.Batcher_#{key}"
  end

  defp get_batch_processor(broadway_name, key, index \\ 0) do
    :"#{broadway_name}.Broadway.BatchProcessor_#{key}_#{index}"
  end

  defp get_rate_limiter(broadway_name) do
    :"#{broadway_name}.RateLimiter"
  end

  defp get_n_producers(broadway_name) do
    Supervisor.count_children(:"#{broadway_name}.Broadway.ProducerSupervisor").workers
  end

  defp get_n_processors(broadway_name) do
    Supervisor.count_children(:"#{broadway_name}.Broadway.ProcessorSupervisor").workers
  end

  defp get_n_consumers(broadway_name, key) do
    Supervisor.count_children(:"#{broadway_name}.Broadway.BatchProcessorSupervisor_#{key}").workers
  end

  defp async_push_messages(producer, list) do
    send(
      producer,
      {:"$gen_call", {self(), make_ref()},
       {Broadway.Topology.ProducerStage, :push_messages, wrap_messages(list)}}
    )
  end

  defp wrap_messages(list) do
    ack = {CallerAcknowledger, {self(), make_ref()}, :ok}
    Enum.map(list, &%Message{data: &1, acknowledger: ack})
  end
end
