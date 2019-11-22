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
        genserver_opts = [name: Module.concat(name, __MODULE__)]
        GenServer.start_link(__MODULE__, {name, rate_limiting_opts}, genserver_opts)
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
    Module.concat(broadway_name, RateLimiterETS)
  end

  @impl true
  def init({name, rate_limiting_opts}) do
    interval = Keyword.fetch!(rate_limiting_opts, :interval)
    allowed = Keyword.fetch!(rate_limiting_opts, :allowed_messages)

    table_name = table_name(name)

    _ets = :ets.new(table_name, [:named_table, :public, :set])
    :ets.insert(table_name, {@row_name, allowed})

    send(self(), :store_producer_names)

    _ = schedule_next_reset(interval, allowed)

    {:ok, {name, table_name, interval, nil}}
  end

  @impl true
  def handle_info({:reset_limit, allowed}, {broadway_name, table_name, interval, producer_names}) do
    was_rate_limited? = get_currently_allowed(table_name) <= 0

    true = :ets.insert(table_name, {@row_name, allowed})

    if was_rate_limited? do
      Enum.each(producer_names, &send(&1, {__MODULE__, :reset_rate_limiting}))
    end

    _ = schedule_next_reset(interval, allowed)

    {:noreply, {broadway_name, table_name, interval, producer_names}}
  end

  def handle_info(:store_producer_names, {broadway_name, table_name, interval, _}) do
    producers = Broadway.producer_names(broadway_name)
    {:noreply, {broadway_name, table_name, interval, producers}}
  end

  defp schedule_next_reset(interval, allowed) do
    _ref = Process.send_after(self(), {:reset_limit, allowed}, interval)
  end
end
