defmodule Broadway.ConfigStorage do
  @moduledoc false
  alias Broadway.ConfigStorage.{Ets, PersistentTerm}

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

  @doc """
  Retrieves the configured module based on the `:config_storage` key.
  """
  @spec get_module() :: module()
  def get_module() do
    Application.get_env(Broadway, :config_storage, :persistent_term)
    |> case do
      :ets -> Ets
      :persistent_term -> PersistentTerm
      mod -> mod
    end
  end

  @doc """
  Retrieves any options set on the `:config_storage` key.
  """
  @spec get_options() :: keyword()
  def get_options() do
    Application.get_env(Broadway, :config_storage_opts) || []
  end
end
