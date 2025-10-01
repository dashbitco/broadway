defmodule Broadway.Topology.ProcessorStageTest do
  use ExUnit.Case, async: true

  test "set custom min and max demand" do
    {:ok, pid} =
      Broadway.Topology.ProcessorStage.start_link(
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

  test "sets process label with topology name, processor key, and partition" do
    topology_name = :test_topology
    processor_key = :default
    partition = 5

    {:ok, pid} =
      Broadway.Topology.ProcessorStage.start_link(
        [
          topology_name: topology_name,
          name: :test_processor,
          module: __MODULE__,
          context: %{},
          type: :consumer,
          terminator: __MODULE__,
          resubscribe: :never,
          processor_config: [min_demand: 1, max_demand: 10],
          processor_key: processor_key,
          producers: [:sample],
          partition: partition,
          batchers: :none,
          producer: nil
        ],
        []
      )

    if Broadway.Process.labels_supported?() do
      label = Process.info(pid, :label)
      assert label == {:label, {:broadway_processor, topology_name, processor_key, partition}}
    else
      # Labels not supported in this Elixir version, skip assertion
      :ok
    end
  end
end
