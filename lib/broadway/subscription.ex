defmodule Broadway.Subscription do
  @moduledoc false

  def subscribe(processe_name, subscribe_to_options) do
    ref = monitor_and_subscribe(processe_name, subscribe_to_options)

    if !ref do
      schedule_resubscribe()
    end

    ref
  end

  def subscribe_all(processes_names, subscribe_to_options) do
    result = monitor_and_subscribe_all(processes_names, subscribe_to_options)

    if Enum.any?(result.failed) do
      schedule_resubscribe()
    end

    {result.refs, result.failed}
  end

  def schedule_resubscribe do
    Process.send_after(self(), :resubscribe, 10)
  end

  defp monitor_and_subscribe_all(processes_names, subscribe_to_options) do
    Enum.reduce(processes_names, %{refs: %{}, failed: []}, fn name, acc ->
      case monitor_and_subscribe(name, subscribe_to_options) do
        nil ->
          %{acc | failed: [name | acc.failed]}

        ref ->
          %{acc | refs: Map.put(acc.refs, ref, name)}
      end
    end)
  end

  defp monitor_and_subscribe(process_name, subscribe_to_options) do
    case Process.whereis(process_name) do
      nil ->
        nil

      pid ->
        ref = Process.monitor(pid)
        opts = [to: pid] ++ subscribe_to_options
        GenStage.async_subscribe(self(), opts)
        ref
    end
  end
end
