defmodule Broadway.Topology.BatchProcessorStageTest do
  use ExUnit.Case, async: true

  test "sets process label with topology name and partition" do
    topology_name = :test_topology
    partition = 3

    {:ok, pid} =
      Broadway.Topology.BatchProcessorStage.start_link(
        [
          topology_name: topology_name,
          name: :test_batch_processor,
          module: __MODULE__,
          context: %{},
          terminator: __MODULE__,
          resubscribe: :never,
          partition: partition,
          batcher: :test_batcher,
          producer: nil
        ],
        []
      )

    if Broadway.Process.labels_supported?() do
      label = Process.info(pid, :label)
      assert label == {:label, {:broadway_batch_processor, topology_name, partition}}
    else
      # Labels not supported in this Elixir version, skip assertion
      :ok
    end
  end
end
