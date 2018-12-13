defmodule Broadway.ProcessorTest do
  use ExUnit.Case

  test "set custom min and max demand" do
    {_, _, config} =
      Broadway.Processor.init(
        module: __MODULE__,
        context: %{},
        publishers_config: [],
        processors_config: [min_demand: 3, max_demand: 6],
        producers: [[]]
      )

    [{_, subscribe_options}] = config[:subscribe_to]

    assert subscribe_options[:min_demand] == 3
    assert subscribe_options[:max_demand] == 6
  end
end
