defmodule Broadway.ConfigStorage.ETS do
  @moduledoc false

  @behaviour Broadway.ConfigStorage

  @table __MODULE__

  # Used in tests.
  def table, do: @table

  @impl true
  def setup do
    :ets.new(@table, [:named_table, :public, :set, {:read_concurrency, true}])
    :ok
  end

  @impl true
  def list do
    :ets.select(@table, [{{:"$1", :_}, [], [:"$1"]}])
  end

  @impl true
  def get(server) do
    case :ets.match(@table, {server, :"$1"}) do
      [[topology]] -> topology
      _ -> nil
    end
  end

  @impl true
  def put(server, topology) do
    :ets.insert(@table, {server, topology})
  end

  @impl true
  def delete(server) do
    :ets.delete(@table, server)
  end
end
