defmodule Broadway.Application do
  @moduledoc false
  use Application

  def start(_type, _args) do
    Supervisor.start_link([Broadway.TermStorage], strategy: :one_for_one, name: __MODULE__)
  end
end
