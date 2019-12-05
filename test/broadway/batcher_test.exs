defmodule Broadway.BatcherTest do
  use ExUnit.Case, async: true

  test "max_demand defaults to batch_size" do
    {:ok, pid} =
      Broadway.Batcher.start_link(
        [
        module: __MODULE__,
        context: %{},
        type: :producer_consumer,
        terminator: __MODULE__,
        resubscribe: :never,
        batcher: :default,
        processors: [:some_processor],
        batch_size: 123,
        batch_timeout: 1000,
        partition: 0
        ],
        []
      )

    %{state: state} = :sys.get_state(pid)
    assert state.subscription_options[:max_demand] == 123
  end
end
