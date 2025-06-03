defmodule Broadway.Application do
  use Application

  def start(_type, _args) do
    config_storage = Broadway.ConfigStorage.get_module()

    if Code.ensure_loaded?(config_storage) and function_exported?(config_storage, :setup, 0) do
      config_storage.setup()
    end

    opts = [strategy: :one_for_one, name: Broadway.Supervisor]
    Supervisor.start_link([], opts)
  end
end
