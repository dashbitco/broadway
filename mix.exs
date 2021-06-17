defmodule Broadway.MixProject do
  use Mix.Project

  @version "0.7.0-dev"
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
      test_coverage: [tool: ExCoveralls]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:gen_stage, "~> 1.0"},
      {:nimble_options, "~> 0.3.0"},
      {:telemetry, ">= 0.4.3 and < 0.5.0"},
      {:ex_doc, ">= 0.19.0", only: :docs},
      {:excoveralls, "~> 0.13.3", only: :test}
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
