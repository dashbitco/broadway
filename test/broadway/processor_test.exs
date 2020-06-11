defmodule Broadway.ProcessorTest do
  use ExUnit.Case, async: true

  test "set custom min and max demand" do
    {:ok, pid} =
      Broadway.Processor.start_link(
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

  describe "telemetry_prefix" do
    test "sets prefix at top level" do
      {:ok, pid} =
        Broadway.Processor.start_link(
          [
            module: __MODULE__,
            context: %{},
            type: :producer_consumer,
            terminator: __MODULE__,
            resubscribe: :never,
            processor_config: [min_demand: 3],
            producers: [:sample],
            partition: 0,
            dispatcher: GenStage.DemandDispatcher,
            telemetry_prefix: [:my_app, :example]
          ],
          []
        )

      %{state: state} = :sys.get_state(pid)
      assert state.telemetry_prefix == [:my_app, :example, :broadway, :processor]
    end

    test "overrides telemetry prefix at processor level" do
      {:ok, pid} =
        Broadway.Processor.start_link(
          [
            module: __MODULE__,
            context: %{},
            type: :producer_consumer,
            terminator: __MODULE__,
            resubscribe: :never,
            processor_config: [telemetry_prefix: [:my_app, :example]],
            producers: [:sample],
            partition: 0,
            dispatcher: GenStage.DemandDispatcher
          ],
          []
        )

      %{state: state} = :sys.get_state(pid)
      assert state.telemetry_prefix == [:my_app, :example, :broadway, :processor]
    end
  end
end
