defmodule Broadway.RateLimiter do
  # TODO: Use :counters once we require Erlang/OTP 22+
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
        producers_names = Keyword.fetch!(opts, :producers_names)
        args = {name, rate_limiting_opts, producers_names}
        GenServer.start_link(__MODULE__, args, name: table_name(name))
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

  def update_rate_limiting(rate_limiter, opts) do
    GenServer.call(rate_limiter, {:update_rate_limiting, opts})
  end

  def get_rate_limiting(rate_limiter) do
    GenServer.call(rate_limiter, :get_rate_limiting)
  end

  @impl true
  def init({broadway_name, rate_limiting_opts, producers_names}) do
    interval = Keyword.fetch!(rate_limiting_opts, :interval)
    allowed = Keyword.fetch!(rate_limiting_opts, :allowed_messages)

    table = :ets.new(table_name(broadway_name), [:named_table, :public, :set])
    :ets.insert(table, {@row_name, allowed})

    _ = schedule_next_reset(interval)

    state = %{
      interval: interval,
      allowed: allowed,
      producers_names: producers_names,
      table: table
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:update_rate_limiting, opts}, _from, state) do
    %{interval: interval, allowed: allowed} = state

    state = %{
      state
      | interval: Keyword.get(opts, :interval, interval),
        allowed: Keyword.get(opts, :allowed_messages, allowed)
    }

    {:reply, :ok, state}
  end

  def handle_call(:get_rate_limiting, _from, state) do
    %{interval: interval, allowed: allowed} = state
    {:reply, %{interval: interval, allowed_messages: allowed}, state}
  end

  @impl true
  def handle_info(:reset_limit, state) do
    %{producers_names: producers_names, interval: interval, allowed: allowed, table: table} =
      state

    true = :ets.insert(table, {@row_name, allowed})

    for name <- producers_names,
        pid = Process.whereis(name),
        is_pid(pid),
        do: send(pid, {__MODULE__, :reset_rate_limiting})

    _ = schedule_next_reset(interval)

    {:noreply, state}
  end

  defp schedule_next_reset(interval) do
    _ref = Process.send_after(self(), :reset_limit, interval)
  end
end
