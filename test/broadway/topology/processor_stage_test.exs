defmodule Broadway.Topology.ProcessorStageTest do
  use ExUnit.Case, async: true

  alias Broadway.Topology.ProcessorStage

  test "set custom min and max demand" do
    {:ok, pid} =
      ProcessorStage.start_link(
        [
          module: __MODULE__,
          context: %{},
          type: :producer_consumer,
          terminator: __MODULE__,
          resubscribe: :never,
          processor_config: [min_demand: 3, max_demand: 6],
          producers: [:sample],
          partition: 0,
          dispatcher: GenStage.DemandDispatcher
        ],
        []
      )

    %{state: state} = :sys.get_state(pid)
    assert state.subscription_options[:min_demand] == 3
    assert state.subscription_options[:max_demand] == 6
  end
end
