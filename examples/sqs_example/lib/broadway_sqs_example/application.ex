defmodule BroadwaySQSExample.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  alias BroadwaySQSExample.Helpers

  use Application

  def start(_type, _args) do
    # create our default queues everywhere but test env
    if Mix.env() != :test do
      Helpers.create_default_queues()
    end

    # List all child processes to be supervised
    children = [
      {BroadwaySQSExample.String, []},
      {BroadwaySQSExample.IntSquared, []}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: BroadwaySQSExample.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
