defmodule Broadway.ConfigStorage do
  @moduledoc """
  Configuration storage behaviour that allows swapping out configuration storage options.
  """

  @doc """
  Optional setup for the configuration storage
  """
  @callback setup() :: :ok

  @doc """
  Lists all broadway names in the config storage
  """
  @callback list() :: [term()]

  @doc """
  Puts the given key value pair in the underlying storage.
  """
  @callback put(server :: term(), value :: %Broadway.Topology{}) :: term()

  @doc """
  Retrieves a configuration from the underlying storage
  """
  @callback get(server :: term()) :: term()

  @doc """
  Deletes a configuration from the underlying storage
  """
  @callback delete(server :: term()) :: boolean()

  @optional_callbacks setup: 0

  @spec get_module() :: module()
  def get_module() do
    Application.get_env(Broadway, :config_storage, PersistentTerm)
    |> case do
      {mod, _opts} -> mod
      mod -> mod
    end
  end

  @spec get_options() :: keyword()
  def get_options() do
    Application.get_env(Broadway, :config_storage, PersistentTerm)
    |> case do
      {_mod, opts} when is_list(opts) -> opts
      _mod -> []
    end
  end
end
