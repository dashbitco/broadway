defmodule Broadway.BatcherTest do
  use ExUnit.Case

  test "max_demand defaults to batch_size" do
    {_, state} =
      Broadway.Batcher.init(
        module: __MODULE__,
        context: %{},
        publisher_key: :default,
        processors: [:some_processor],
        batch_size: 123,
        batch_timeout: 1000
      )

    assert state.subscribe_to_options[:max_demand] == 123
  end
end
