defmodule Broadway.TermStorage do
  @moduledoc """
  A simple term storage to avoid passing large amounts of data between processes.

  If you have a large amount of data and you want to avoid passing it between
  processes, you can use the `TermStorage`. The `TermStorage` creates a unique
  reference for it, allowing you pass the reference around instead of the term.

  If the same term is put multiple times, it is stored only once, avoiding
  generating garbage. However, the stored terms are never removed.
  A common use case for this feature is in Broadway.Producer, which may need
  to pass a lot of information to acknowledge messages. With this module, you
  can store those terms when the producer starts and only pass the reference
  between messages:

      iex> ref = Broadway.TermStorage.put({:foo, :bar, :baz}) # On init
      iex> Broadway.TermStorage.get!(ref) # On ack
      {:foo, :bar, :baz}

  """

  use GenServer
  @name __MODULE__

  @doc false
  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: @name)
  end

  @doc """
  Gets a previously stored term.
  """
  def get!(ref) when is_reference(ref) do
    :ets.lookup_element(@name, ref, 2)
  end

  @doc """
  Puts a term.
  """
  def put(term) do
    find_by_term(term) || GenServer.call(@name, {:put, term}, :infinity)
  end

  # Callbacks

  @impl true
  def init(:ok) do
    ets = :ets.new(@name, [:named_table, :protected, :set, read_concurrency: true])
    {:ok, ets}
  end

  @impl true
  def handle_call({:put, term}, _from, state) do
    if ref = find_by_term(term) do
      {:reply, ref, state}
    else
      ref = make_ref()
      :ets.insert(@name, {ref, term})
      {:reply, ref, state}
    end
  end

  defp find_by_term(term) do
    case :ets.match(@name, {:"$1", term}) do
      [[ref]] -> ref
      [] -> nil
    end
  end
end
