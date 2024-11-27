defmodule Broadway.ConfigStorage.Ets do
  alias Broadway.ConfigStorage
  @behaviour ConfigStorage

  @default_table :broadway_configs

  def default_table(), do: @default_table

  @impl ConfigStorage
  def setup do
    if :undefined == :ets.whereis(table()) do
      :ets.new(table(), [:named_table, :public, :set, {:read_concurrency, true}])
    end

    :ok
  end

  @impl ConfigStorage
  def list do
    :ets.select(table(), [{{:"$1", :_}, [], [:"$1"]}])
  end

  @impl ConfigStorage
  def get(server) do
    case :ets.match(table(), {server, :"$1"}) do
      [[topology]] -> topology
      _ -> nil
    end
  end

  @impl ConfigStorage
  def put(server, topology) do
    :ets.insert(table(), {server, topology})
  end

  @impl ConfigStorage
  def delete(server) do
    :ets.delete(table(), server)
  end

  defp table() do
    opts = ConfigStorage.get_options()
    Keyword.get(opts, :table_name, @default_table)
  end
end
