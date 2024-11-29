defmodule Broadway.ConfigStorage.Ets do
  @moduledoc false
  @behaviour Broadway.ConfigStorage

  def table(), do: __MODULE__

  @impl true
  def setup do
    if :undefined == :ets.whereis(table()) do
      :ets.new(table(), [:named_table, :public, :set, {:read_concurrency, true}])
    end

    :ok
  end

  @impl true
  def list do
    :ets.select(table(), [{{:"$1", :_}, [], [:"$1"]}])
  end

  @impl true
  def get(server) do
    case :ets.match(table(), {server, :"$1"}) do
      [[topology]] -> topology
      _ -> nil
    end
  end

  @impl true
  def put(server, topology) do
    :ets.insert(table(), {server, topology})
  end

  @impl true
  def delete(server) do
    :ets.delete(table(), server)
  end
end
