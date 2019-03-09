defmodule Broadway.BatcherTest do
  use ExUnit.Case, async: true

  test "max_demand defaults to batch_size" do
    {_, state, _} =
      Broadway.Batcher.init(
        module: __MODULE__,
        context: %{},
        type: :producer_consumer,
        terminator: __MODULE__,
        resubscribe: :never,
        batcher: :default,
        processors: [:some_processor],
        batch_size: 123,
        batch_timeout: 1000
      )

    assert state.subscription_options[:max_demand] == 123
  end
end
