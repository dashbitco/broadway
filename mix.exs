defmodule Broadway.MixProject do
  use Mix.Project

  @version "1.2.1"
  @description "Build concurrent and multi-stage data ingestion and data processing pipelines"

  def project do
    [
      app: :broadway,
      version: @version,
      elixir: "~> 1.7",
      name: "Broadway",
      description: @description,
      deps: deps(),
      docs: docs(),
      package: package(),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [docs: :docs]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      env: [config_storage: :persistent_term],
      mod: {Broadway.Application, []}
    ]
  end

  defp deps do
    [
      {:gen_stage, "~> 1.0"},
      {:nimble_options, "~> 0.3.7 or ~> 0.4 or ~> 1.0"},
      {:telemetry, "~> 0.4.3 or ~> 1.0"},

      # Dev/test dependencies.
      {:castore, "~> 1.0", only: :test},
      {:ex_doc, ">= 0.19.0", only: :docs},
      {:excoveralls, "~> 0.18.0", only: :test}
    ]
  end

  defp docs do
    [
      main: "introduction",
      source_ref: "v#{@version}",
      source_url: "https://github.com/dashbitco/broadway",
      extra_section: "Guides",
      extras: [
        "guides/examples/introduction.md",
        "guides/examples/amazon-sqs.md",
        "guides/examples/apache-kafka.md",
        "guides/examples/google-cloud-pubsub.md",
        "guides/examples/rabbitmq.md",
        "guides/examples/custom-producers.md",
        "guides/internals/architecture.md"
      ],
      groups_for_extras: [
        Examples: Path.wildcard("guides/examples/*.md"),
        Internals: Path.wildcard("guides/internals/*.md")
      ],
      groups_for_modules: [
        # Ungrouped Modules:
        #
        # Broadway
        # Broadway.Message
        # Broadway.BatchInfo

        Acknowledgement: [
          Broadway.Acknowledger,
          Broadway.CallerAcknowledger,
          Broadway.NoopAcknowledger
        ],
        Producers: [
          Broadway.Producer,
          Broadway.DummyProducer
        ]
      ]
    ]
  end

  defp package do
    %{
      licenses: ["Apache-2.0"],
      maintainers: ["Marlus Saraiva", "José Valim"],
      links: %{"GitHub" => "https://github.com/dashbitco/broadway"}
    }
  end
end
