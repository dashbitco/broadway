defmodule Broadway.ConfigStorage.Ets do
  @moduledoc """
  An ETS-backed configuration storage. Only use this if performance improvements over the default `:persistent_term`-based storage is needed.

  To use this configuration storage option, set your application config.exs as so:

  ```elixir
  config Broadway, config_storage: Broadway.ConfigStorage.Ets
  ```

  To pass options, use a tuple with a keyword list as so:

  ```elixir
  config Broadway, config_storage: {Broadway.ConfigStorage.Ets, table_name: :my_table}
  ```

  Default table is `:broadway_configs`.

  Accepted options:
  - `:table_name` - configure the table name.

  ## Performance Improvements
  `:persistent_term` will trigger a global GC on each `put` or `erase`. For situations where there are a large number of dynamically created Broadway pipelines that are created or removed, this may result in the global GC being triggered multiple times. If there is a large number of processes, this may cause the system to be less responsive until all heaps have been scanned.

  As `Broadway.ConfigStorage.PersistentTerm` does not perform an erase when the Broadway server process goes down, it may result in memory buildup over time within the `:persistent_term` hash table, especially when dynamic names are used for the Broadway servers.

  Furthermore, the speed of storing and updating using `:persistent_term` is proportional to the number of already-created terms in the hash table, as the hash table (and term) is copied.

  Using `Broadway.ConfigStorage.Ets` will allow for a large number of Broadway server configurations to be stored and fetched without the associated performance tradeoffs that `:persistent_term` has.
  """
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
