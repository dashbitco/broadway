defmodule Broadway.ProducerTest do
  use ExUnit.Case

  alias Broadway.{Producer, Message}

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
      %Message{data: "#{event}#{text}"}
    end

    defp wrap_message(data) do
      %Message{data: data}
    end
  end

  defmodule ProducerWithOutTerminate do
    use GenStage

    def init(_), do: {:producer, nil}
  end

  setup do
    %{state: %{module: FakeProducer, transformer: nil, module_state: nil}}
  end

  describe "wrap handle_demand" do
    test "returning {:noreply, [event], new_state}", %{state: state} do
      state = %{state | module_state: :return_no_reply}
      new_state = %{state | module_state: :new_module_state}

      assert {:noreply, [%Message{data: 10}], ^new_state} = Producer.handle_demand(10, state)
    end

    test "returning {:noreply, [event], new_state, :hibernate}", %{state: state} do
      state = %{state | module_state: :return_no_reply_with_hibernate}
      new_state = %{state | module_state: :new_module_state}

      assert {:noreply, [%Message{data: 10}], ^new_state, :hibernate} =
               Producer.handle_demand(10, state)
    end

    test "returning {:stop, reason, new_state}", %{state: state} do
      state = %{state | module_state: :return_stop}
      new_state = %{state | module_state: :new_module_state}

      assert Producer.handle_demand(10, state) == {:stop, "error_on_demand_10", new_state}
    end

    test "raise an error if a message is not a %Message{}", %{state: state} do
      state = %{state | module_state: :do_not_wrap_messages}

      assert_raise RuntimeError,
                   ~r/the produced message is invalid/,
                   fn -> Producer.handle_demand(10, state) end
    end

    test "transform events into %Message{} structs using a transformer", %{state: state} do
      transformer = {FakeProducer, :transformer, [concat: " ok"]}
      state = %{state | module_state: :do_not_wrap_messages, transformer: transformer}
      new_state = %{state | module_state: :new_module_state}

      assert {:noreply, [%Message{data: "10 ok"}], ^new_state} = Producer.handle_demand(10, state)
    end
  end

  describe "wrap handle_info" do
    test "returning {:noreply, [event], new_state}", %{state: state} do
      state = %{state | module_state: :return_no_reply}
      new_state = %{state | module_state: :new_module_state}

      assert {:noreply, [%Message{data: :a_message}], ^new_state} =
               Producer.handle_info(:a_message, state)
    end

    test "returning {:noreply, [event], new_state, :hibernate}", %{state: state} do
      state = %{state | module_state: :return_no_reply_with_hibernate}
      new_state = %{state | module_state: :new_module_state}

      assert {:noreply, [%Message{data: :a_message}], ^new_state, :hibernate} =
               Producer.handle_info(:a_message, state)
    end

    test "returning {:stop, reason, new_state}", %{state: state} do
      state = %{state | module_state: :return_stop}
      new_state = %{state | module_state: :new_module_state}

      assert Producer.handle_info(:a_message, state) == {:stop, "error_on_a_message", new_state}
    end

    test "raise an error if a message is not a %Message{}", %{state: state} do
      state = %{state | module_state: :do_not_wrap_messages}

      assert_raise RuntimeError,
                   ~r/the produced message is invalid/,
                   fn -> Producer.handle_info(:not_a_message, state) end
    end

    test "transform events into %Message{} structs using a transformer", %{state: state} do
      transformer = {FakeProducer, :transformer, [concat: " ok"]}
      state = %{state | module_state: :do_not_wrap_messages, transformer: transformer}
      new_state = %{state | module_state: :new_module_state}

      assert {:noreply, [%Message{data: "10 ok"}], ^new_state} = Producer.handle_info(10, state)
    end
  end

  describe "wrap terminate" do
    test "forward call to wrapped module" do
      state = %{module: FakeProducer, module_state: :module_state}

      assert Producer.terminate(:normal, state) == {:normal, :module_state}

      assert Producer.terminate({:shutdown, :a_term}, state) ==
               {{:shutdown, :a_term}, :module_state}
    end

    test "returns :ok when the wrapped module doesn't define a terminate/2 callback" do
      state = %{module: ProducerWithOutTerminate, module_state: :module_state}

      assert Producer.terminate(:normal, state) == :ok
    end
  end
end
