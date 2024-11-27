defmodule Broadway.ConfigStorage.PersistentTerm do
  @moduledoc """
  A `:persistent_term` backed configuration storage.

  This configuration storage is used by default.

  ```elixir
  config Broadway, config_storage: Broadway.ConfigStorage.PersistentTerm
  ```

  Configurations are not deleted when the process goes down, so as to avoid a global GC.

  """
  @behaviour Broadway.ConfigStorage

  @impl Broadway.ConfigStorage
  def setup do
    unless Code.ensure_loaded?(:persistent_term) do
      require Logger
      Logger.error("Broadway requires Erlang/OTP 21.3+")
      raise "Broadway requires Erlang/OTP 21.3+"
    end

    :ok
  end

  @impl Broadway.ConfigStorage
  def list do
    for {{Broadway, name}, %Broadway.Topology{}} <- :persistent_term.get() do
      name
    end
  end

  @impl Broadway.ConfigStorage
  def get(server) do
    :persistent_term.get({Broadway, server}, nil)
  end

  @impl Broadway.ConfigStorage
  def put(server, topology) do
    :persistent_term.put({Broadway, server}, topology)
  end

  @impl Broadway.ConfigStorage
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
