defmodule Broadway.MixProject do
  use Mix.Project

  @version "0.6.0"
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
      {:gen_stage, "~> 1.0"},
      {:telemetry, "~> 0.4.0"},
      {:ex_doc, ">= 0.19.0", only: :docs}
    ]
  end

  defp docs do
    [
      main: "Broadway",
      source_ref: "v#{@version}",
      source_url: "https://github.com/dashbitco/broadway",
      extra_section: "Guides",
      extras: [
        "guides/examples/Amazon SQS.md",
        "guides/examples/Apache Kafka.md",
        "guides/examples/Google Cloud PubSub.md",
        "guides/examples/RabbitMQ.md",
        "guides/examples/Custom Producers.md",
        "guides/internals/Architecture.md"
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
          Broadway.DummyProducer,
          Broadway.TermStorage
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
