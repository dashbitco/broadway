defmodule Broadway.BatcherTest do
  use ExUnit.Case

  test "default min and max demand" do
    {_, _, config} =
      Broadway.Batcher.init(
        module: __MODULE__,
        context: %{},
        publisher_key: :default,
        processors: [:some_processor]
      )

    [{_, subscribe_options}] = config[:subscribe_to]

    assert subscribe_options[:min_demand] == 2
    assert subscribe_options[:max_demand] == 4
  end

  test "set custom min and max demand" do
    {_, _, config} =
      Broadway.Batcher.init(
        module: __MODULE__,
        context: %{},
        publisher_key: :default,
        processors: [:some_processor],
        min_demand: 3,
        max_demand: 6
      )

    [{_, subscribe_options}] = config[:subscribe_to]

    assert subscribe_options[:min_demand] == 3
    assert subscribe_options[:max_demand] == 6
  end
end
