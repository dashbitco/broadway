defmodule Broadway.BatcherTest do
  use ExUnit.Case

  test "set custom min and max demand" do
    {_, state} =
      Broadway.Batcher.init(
        module: __MODULE__,
        context: %{},
        publisher_key: :default,
        processors: [:some_processor],
        batch_timeout: 1000,
        min_demand: 3,
        max_demand: 6
      )

    assert state.subscribe_to_options[:min_demand] == 3
    assert state.subscribe_to_options[:max_demand] == 6
  end
end
