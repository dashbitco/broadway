defmodule Broadway.ConfigStorage.PersistentTerm do
  @moduledoc false
  @behaviour Broadway.ConfigStorage

  @impl true
  def setup do
    unless Code.ensure_loaded?(:persistent_term) do
      require Logger
      Logger.error("Broadway requires Erlang/OTP 21.3+")
      raise "Broadway requires Erlang/OTP 21.3+"
    end

    :ok
  end

  @impl true
  def list do
    for {{Broadway, name}, %Broadway.Topology{}} <- :persistent_term.get() do
      name
    end
  end

  @impl true
  def get(server) do
    :persistent_term.get({Broadway, server}, nil)
  end

  @impl true
  def put(server, topology) do
    :persistent_term.put({Broadway, server}, topology)
  end

  @impl true
  def delete(_server) do
    # We don't delete from persistent term on purpose. Since the process is
    # named, we can assume it does not start dynamically, so it will either
    # restart or the amount of memory it uses is negligibla to justify the
    # process purging done by persistent_term. If the repo is restarted and
    # stores the same metadata, then no purging happens either.
    # :persistent_term.erase({Broadway, server})
    true
  end
end
