defmodule Broadway.Topology.ProducerStageTest do
  use ExUnit.Case, async: true

  alias Broadway.Message
  alias Broadway.Topology.ProducerStage

  defmodule FakeProducer do
    use GenStage

    def init(_), do: {:producer, nil}

    def handle_demand(demand, :return_no_reply) do
      {:noreply, [wrap_message(demand)], :new_module_state}
    end

    def handle_demand(demand, :return_no_reply_with_hibernate) do
      {:noreply, [wrap_message(demand)], :new_module_state, :hibernate}
    end

    def handle_demand(demand, :return_stop) do
      {:stop, "error_on_demand_#{demand}", :new_module_state}
    end

    def handle_demand(demand, :do_not_wrap_messages) do
      {:noreply, [demand], :new_module_state}
    end

    def handle_info(message, :return_no_reply) do
      {:noreply, [wrap_message(message)], :new_module_state}
    end

    def handle_info(message, :return_no_reply_with_hibernate) do
      {:noreply, [wrap_message(message)], :new_module_state, :hibernate}
    end

    def handle_info(message, :return_stop) do
      {:stop, "error_on_#{message}", :new_module_state}
    end

    def handle_info(message, :do_not_wrap_messages) do
      {:noreply, [message], :new_module_state}
    end

    def terminate(reason, state) do
      {reason, state}
    end

    def transformer(event, concat: text) do
      %Message{data: "#{event}#{text}", acknowledger: {__MODULE__, event}}
    end

    defp wrap_message(data) do
      %Message{data: data, acknowledger: {__MODULE__, data}}
    end
  end

  defmodule ProducerWithOutTerminate do
    use GenStage

    def init(_), do: {:producer, nil}
  end

  defmodule ProducerWithBadReturn do
    use GenStage

    def init(_), do: {:consumer, nil}
  end

  setup do
    %{
      state: %{
        module: FakeProducer,
        transformer: nil,
        module_state: nil,
        rate_limiting: nil
      }
    }
  end

  test "init with bad return" do
    args = %{module: {ProducerWithBadReturn, []}, broadway: []}

    assert ProducerStage.init({args, _index = 0}) ==
             {:stop, {:bad_return_value, {:consumer, nil}}}
  end

  describe "wrap handle_demand" do
    test "returning {:noreply, [event], new_state}", %{state: state} do
      state = %{state | module_state: :return_no_reply}
      new_state = %{state | module_state: :new_module_state}

      assert {:noreply, [%Message{data: 10}], ^new_state} = ProducerStage.handle_demand(10, state)
    end

    test "returning {:noreply, [event], new_state, :hibernate}", %{state: state} do
      state = %{state | module_state: :return_no_reply_with_hibernate}
      new_state = %{state | module_state: :new_module_state}

      assert {:noreply, [%Message{data: 10}], ^new_state, :hibernate} =
               ProducerStage.handle_demand(10, state)
    end

    test "returning {:stop, reason, new_state}", %{state: state} do
      state = %{state | module_state: :return_stop}
      new_state = %{state | module_state: :new_module_state}

      assert ProducerStage.handle_demand(10, state) == {:stop, "error_on_demand_10", new_state}
    end

    test "raise an error if a message is not a %Message{}", %{state: state} do
      state = %{state | module_state: :do_not_wrap_messages}

      assert_raise RuntimeError,
                   ~r/the produced message is invalid/,
                   fn -> ProducerStage.handle_demand(10, state) end
    end

    test "transform events into %Message{} structs using a transformer", %{state: state} do
      transformer = {FakeProducer, :transformer, [concat: " ok"]}
      state = %{state | module_state: :do_not_wrap_messages, transformer: transformer}
      new_state = %{state | module_state: :new_module_state}

      assert {:noreply, [%Message{data: "10 ok"}], ^new_state} =
               ProducerStage.handle_demand(10, state)
    end
  end

  describe "wrap handle_info" do
    test "returning {:noreply, [event], new_state}", %{state: state} do
      state = %{state | module_state: :return_no_reply}
      new_state = %{state | module_state: :new_module_state}

      assert {:noreply, [%Message{data: :a_message}], ^new_state} =
               ProducerStage.handle_info(:a_message, state)
    end

    test "returning {:noreply, [event], new_state, :hibernate}", %{state: state} do
      state = %{state | module_state: :return_no_reply_with_hibernate}
      new_state = %{state | module_state: :new_module_state}

      assert {:noreply, [%Message{data: :a_message}], ^new_state, :hibernate} =
               ProducerStage.handle_info(:a_message, state)
    end

    test "returning {:stop, reason, new_state}", %{state: state} do
      state = %{state | module_state: :return_stop}
      new_state = %{state | module_state: :new_module_state}

      assert ProducerStage.handle_info(:a_message, state) ==
               {:stop, "error_on_a_message", new_state}
    end

    test "raise an error if a message is not a %Message{}", %{state: state} do
      state = %{state | module_state: :do_not_wrap_messages}

      assert_raise RuntimeError,
                   ~r/the produced message is invalid/,
                   fn -> ProducerStage.handle_info(:not_a_message, state) end
    end

    test "transform events into %Message{} structs using a transformer", %{state: state} do
      transformer = {FakeProducer, :transformer, [concat: " ok"]}
      state = %{state | module_state: :do_not_wrap_messages, transformer: transformer}
      new_state = %{state | module_state: :new_module_state}

      assert {:noreply, [%Message{data: "10 ok"}], ^new_state} =
               ProducerStage.handle_info(10, state)
    end
  end

  describe "wrap terminate" do
    test "forward call to wrapped module" do
      state = %{module: FakeProducer, module_state: :module_state}

      assert ProducerStage.terminate(:normal, state) == {:normal, :module_state}

      assert ProducerStage.terminate({:shutdown, :a_term}, state) ==
               {{:shutdown, :a_term}, :module_state}
    end

    test "returns :ok when the wrapped module doesn't define a terminate/2 callback" do
      state = %{module: ProducerWithOutTerminate, module_state: :module_state}

      assert ProducerStage.terminate(:normal, state) == :ok
    end
  end

  test "sets process label with topology name and index" do
    topology_name = :test_topology
    index = 2

    args = %{
      module: {FakeProducer, []},
      broadway: [name: topology_name, index: index],
      transformer: nil,
      dispatcher: GenStage.DemandDispatcher,
      rate_limiter: nil
    }

    {:ok, pid} = ProducerStage.start_link(args, index)

    if Broadway.Process.labels_supported?() do
      label = Process.info(pid, :label)
      assert label == {:label, {:broadway_producer, topology_name, index}}
    else
      # Labels not supported in this Elixir version, skip assertion
      :ok
    end
  end
end
