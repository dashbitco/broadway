defmodule Broadway.ProducerTest do
  use ExUnit.Case

  alias Broadway.Producer

  defmodule FakeProducer do
    use GenStage

    def init(_), do: {:producer, nil}

    def handle_demand(demand, :return_no_reply) do
      {:noreply, [demand], :new_module_state}
    end

    def handle_demand(demand, :return_no_reply_with_hibernate) do
      {:noreply, [demand], :new_module_state, :hibernate}
    end

    def handle_demand(demand, :return_stop) do
      {:stop, "error_on_demand_#{demand}", :new_module_state}
    end

    def handle_info(message, :return_no_reply) do
      {:noreply, [message], :new_module_state}
    end

    def handle_info(message, :return_no_reply_with_hibernate) do
      {:noreply, [message], :new_module_state, :hibernate}
    end

    def handle_info(message, :return_stop) do
      {:stop, "error_on_#{message}", :new_module_state}
    end

    def terminate(reason, state) do
      {reason, state}
    end
  end

  defmodule ProducerWithOutTerminate do
    use GenStage

    def init(_), do: {:producer, nil}
  end

  describe "wrap handle_demand" do
    test "returning {:noreply, [event], new_state}" do
      state = %{module: FakeProducer, module_state: :return_no_reply}
      new_state = %{module: FakeProducer, module_state: :new_module_state}

      assert Producer.handle_demand(10, state) == {:noreply, [10], new_state}
    end

    test "returning {:noreply, [event], new_state, :hibernate}" do
      state = %{module: FakeProducer, module_state: :return_no_reply_with_hibernate}
      new_state = %{module: FakeProducer, module_state: :new_module_state}

      assert Producer.handle_demand(10, state) == {:noreply, [10], new_state, :hibernate}
    end

    test "returning {:stop, reason, new_state}" do
      state = %{module: FakeProducer, module_state: :return_stop}
      new_state = %{module: FakeProducer, module_state: :new_module_state}

      assert Producer.handle_demand(10, state) == {:stop, "error_on_demand_10", new_state}
    end
  end

  describe "wrap handle_info" do
    test "returning {:noreply, [event], new_state}" do
      state = %{module: FakeProducer, module_state: :return_no_reply}
      new_state = %{module: FakeProducer, module_state: :new_module_state}

      assert Producer.handle_info(:a_message, state) == {:noreply, [:a_message], new_state}
    end

    test "returning {:noreply, [event], new_state, :hibernate}" do
      state = %{module: FakeProducer, module_state: :return_no_reply_with_hibernate}
      new_state = %{module: FakeProducer, module_state: :new_module_state}

      assert Producer.handle_info(:a_message, state) ==
               {:noreply, [:a_message], new_state, :hibernate}
    end

    test "returning {:stop, reason, new_state}" do
      state = %{module: FakeProducer, module_state: :return_stop}
      new_state = %{module: FakeProducer, module_state: :new_module_state}

      assert Producer.handle_info(:a_message, state) == {:stop, "error_on_a_message", new_state}
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
