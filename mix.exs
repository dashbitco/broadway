defmodule Broadway.MixProject do
  use Mix.Project

  @version "0.2.0"
  @description "Build concurrent and multi-stage data ingestion and data processing pipelines"

  def project do
    [
      app: :broadway,
      version: "0.1.0",
      elixir: "~> 1.5",
      name: "Broadway",
      description: @description,
      deps: deps(),
      docs: docs(),
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Broadway.Application, []}
    ]
  end

  defp deps do
    [
      {:gen_stage, "~> 0.14"},
      {:ex_doc, ">= 0.19.0", only: :docs}
    ]
  end

  defp docs do
    [
      main: "Broadway",
      source_ref: "v#{@version}",
      source_url: "https://github.com/plataformatec/broadway",
      extra_section: "Guides",
      extras: [
        "guides/examples/Amazon SQS.md",
        "guides/examples/RabbitMQ.md",
        "guides/examples/Custom producers.md",
        "guides/internals/Architecture.md"
      ],
      groups_for_extras: [
        Examples: Path.wildcard("guides/examples/*.md"),
        Internals: Path.wildcard("guides/internals/*.md")
      ]
    ]
  end

  defp package do
    %{
      licenses: ["Apache 2"],
      maintainers: ["Marlus Saraiva", "José Valim"],
      links: %{"GitHub" => "https://github.com/plataformatec/broadway"}
    }
  end
end
