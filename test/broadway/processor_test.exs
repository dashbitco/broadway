defmodule Broadway.ProcessorTest do
  use ExUnit.Case, async: true

  test "set custom min and max demand" do
    {_, state, _} =
      Broadway.Processor.init(
        module: __MODULE__,
        context: %{},
        type: :producer_consumer,
        terminator: __MODULE__,
        resubscribe: :never,
        processor_config: [min_demand: 3, max_demand: 6],
        producers: [:sample]
      )

    assert state.subscription_options[:min_demand] == 3
    assert state.subscription_options[:max_demand] == 6
  end
end
