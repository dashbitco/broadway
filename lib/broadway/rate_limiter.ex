defmodule Broadway.RateLimiter do
  @moduledoc false

  use GenServer
  @row_name :rate_limit_counter

  def start_link(opts) do
    case Keyword.fetch!(opts, :rate_limiting) do
      # If we don't have rate limiting options, we don't even need to start this rate
      # limiter process.
      nil ->
        :ignore

      rate_limiting_opts ->
        name = Keyword.fetch!(opts, :name)
        GenServer.start_link(__MODULE__, {name, rate_limiting_opts}, name: table_name(name))
    end
  end

  def rate_limit(table_name, amount)
      when is_atom(table_name) and is_integer(amount) and amount > 0 do
    :ets.update_counter(table_name, @row_name, -amount)
  end

  def get_currently_allowed(table_name) when is_atom(table_name) do
    :ets.lookup_element(table_name, @row_name, 2)
  end

  def table_name(broadway_name) when is_atom(broadway_name) do
    Module.concat(broadway_name, RateLimiter)
  end

  @impl true
  def init({broadway_name, rate_limiting_opts}) do
    interval = Keyword.fetch!(rate_limiting_opts, :interval)
    allowed = Keyword.fetch!(rate_limiting_opts, :allowed_messages)

    table = :ets.new(table_name(broadway_name), [:named_table, :public, :set])
    :ets.insert(table, {@row_name, allowed})

    # We need to fetch (and store in the state) the producer names after we start
    # because now the producers haven't been started yet.
    send(self(), {:store_producer_names, broadway_name})

    _ = schedule_next_reset(interval)

    state = %{
      interval: interval,
      allowed: allowed,
      producers: [],
      table: table
    }

    {:ok, state}
  end

  @impl true
  def handle_info(:reset_limit, state) do
    %{producers: producers, interval: interval, allowed: allowed, table: table} = state

    true = :ets.insert(table, {@row_name, allowed})

    :ok = Enum.each(producers, &send(&1, {__MODULE__, :reset_rate_limiting}))

    _ = schedule_next_reset(interval)

    {:noreply, state}
  end

  def handle_info({:store_producer_names, broadway_name}, state) do
    producers = Broadway.producer_names(broadway_name)
    {:noreply, %{state | producers: producers}}
  end

  defp schedule_next_reset(interval) do
    _ref = Process.send_after(self(), :reset_limit, interval)
  end
end
