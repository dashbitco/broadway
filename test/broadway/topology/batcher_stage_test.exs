defmodule Broadway.Topology.BatcherStageTest do
  use ExUnit.Case, async: true

  test "max_demand defaults to batch_size" do
    {:ok, pid} =
      Broadway.Topology.BatcherStage.start_link(
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

  test "sets process label with topology name and batcher key" do
    topology_name = :test_topology
    batcher_key = :my_batcher

    {:ok, pid} =
      Broadway.Topology.BatcherStage.start_link(
        [
          topology_name: topology_name,
          name: :test_batcher,
          context: %{},
          terminator: __MODULE__,
          resubscribe: :never,
          batcher: batcher_key,
          processors: [:some_processor],
          batch_size: 10,
          batch_timeout: 1000,
          partition: batcher_key
        ],
        []
      )

    if Broadway.Process.labels_supported?() do
      label = Process.info(pid, :label)
      assert label == {:label, {:broadway_batcher, topology_name, batcher_key}}
    else
      # Labels not supported in this Elixir version, skip assertion
      :ok
    end
  end
end
