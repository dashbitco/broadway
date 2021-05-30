defmodule Broadway.Topology.RateLimiter do
  @moduledoc false

  use GenServer

  @atomics_index 1

  def start_link(opts) do
    case Keyword.fetch!(opts, :rate_limiting) do
      # If we don't have rate limiting options, we don't even need to start this rate
      # limiter process.
      nil ->
        :ignore

      rate_limiting_opts ->
        name = Keyword.fetch!(opts, :name)
        producers_names = Keyword.fetch!(opts, :producers_names)
        args = {rate_limiting_opts, producers_names}
        GenServer.start_link(__MODULE__, args, name: name)
    end
  end

  def rate_limit(counter, amount)
      when is_reference(counter) and is_integer(amount) and amount > 0 do
    :atomics.sub_get(counter, @atomics_index, amount)
  end

  def get_currently_allowed(counter) when is_reference(counter) do
    :atomics.get(counter, @atomics_index)
  end

  def update_rate_limiting(rate_limiter, opts) do
    GenServer.call(rate_limiter, {:update_rate_limiting, opts})
  end

  def get_rate_limiting(rate_limiter) do
    GenServer.call(rate_limiter, :get_rate_limiting)
  end

  def get_rate_limiter_ref(rate_limiter) do
    GenServer.call(rate_limiter, :get_rate_limiter_ref)
  end

  @impl true
  def init({rate_limiting_opts, producers_names}) do
    interval = Keyword.fetch!(rate_limiting_opts, :interval)
    allowed = Keyword.fetch!(rate_limiting_opts, :allowed_messages)

    counter = :atomics.new(@atomics_index, [])
    :atomics.put(counter, @atomics_index, allowed)

    _ = schedule_next_reset(interval)

    state = %{
      interval: interval,
      allowed: allowed,
      producers_names: producers_names,
      counter: counter
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

  def handle_call(:get_rate_limiter_ref, _from, %{counter: counter} = state) do
    {:reply, counter, state}
  end

  @impl true
  def handle_info(:reset_limit, state) do
    %{producers_names: producers_names, interval: interval, allowed: allowed, counter: counter} =
      state

    :atomics.put(counter, @atomics_index, allowed)

    for name <- producers_names,
        pid = GenServer.whereis(name),
        is_pid(pid),
        do: send(pid, {__MODULE__, :reset_rate_limiting})

    _ = schedule_next_reset(interval)

    {:noreply, state}
  end

  defp schedule_next_reset(interval) do
    _ref = Process.send_after(self(), :reset_limit, interval)
  end
end
