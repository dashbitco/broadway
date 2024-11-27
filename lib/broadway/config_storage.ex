defmodule Broadway.ConfigStorage do
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
end
