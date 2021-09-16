defmodule Broadway.Options do
  @moduledoc false

  definition = [
    name: [
      required: true,
      type: {:custom, __MODULE__, :validate_name, []},
      doc: """
      Used for name registration. When an atom, all processes/stages
      created will be named using this value as prefix.
      """
    ],
    shutdown: [
      type: :pos_integer,
      default: 30000,
      doc: """
      Optional. The time in milliseconds given for Broadway to
      gracefully shutdown without discarding events. Defaults to `30_000` (millisecond).
      """
    ],
    max_restarts: [type: :non_neg_integer, default: 3],
    max_seconds: [type: :pos_integer, default: 5],
    resubscribe_interval: [
      type: :non_neg_integer,
      default: 100,
      doc: """
      The interval in milliseconds that
      processors wait until they resubscribe to a failed producers. Defaults
      to `100` (millisecond).
      """
    ],
    context: [
      type: :any,
      default: :context_not_set,
      doc: """
      A user defined data structure that will be passed to handle_message/3 and handle_batch/4.
      """
    ],
    producer: [
      required: true,
      type: :non_empty_keyword_list,
      doc: """
      A keyword list of options. See ["Producers options"](#start_link/2-producers-options)
      section below. Only a single producer is allowed.
      """,
      subsection: """
      ### Producers options

      The producer options allow users to set up the producer.

      The available options are:
      """,
      keys: [
        module: [
          required: true,
          type: :mod_arg,
          doc: """
          A tuple representing a GenStage producer.
          The tuple format should be `{mod, arg}`, where `mod` is the module
          that implements the GenStage behaviour and `arg` the argument that will
          be passed to the `init/1` callback of the producer. See `Broadway.Producer`
          for more information.
          """
        ],
        concurrency: [
          type: :pos_integer,
          default: 1,
          doc: """
          The number of concurrent producers that
          will be started by Broadway. Use this option to control the concurrency
          level of each set of producers. The default value is `1`.
          """
        ],
        transformer: [
          type: :mfa,
          default: nil,
          doc: """
          A tuple representing a transformer that translates a produced GenStage event into a
          `%Broadway.Message{}`. The tuple format should be `{mod, fun, opts}` and the function
          should have the following spec `(event :: term, opts :: term) :: Broadway.Message.t`
          This function must be used sparingly and exclusively to convert regular
          messages into `Broadway.Message`. That's because a failure in the
          `:transformer` callback will cause the whole producer to terminate,
          possibly leaving unacknowledged messages along the way.
          """
        ],
        spawn_opt: [
          type: :keyword_list,
          doc: """
          Overrides the top-level `:spawn_opt`.
          """
        ],
        hibernate_after: [
          type: :pos_integer,
          doc: """
          Overrides the top-level `:hibernate_after`.
          """
        ],
        rate_limiting: [
          type: :non_empty_keyword_list,
          doc: """
          A list of options to enable and configure rate limiting for producing.
          If this option is present, rate limiting is enabled, otherwise it isn't.
          Rate limiting refers to the rate at which producers will forward
          messages to the rest of the pipeline. The rate limiting is applied to
          and shared by all producers within the time limit.
          The following options are supported:
          """,
          keys: [
            allowed_messages: [
              required: true,
              type: :pos_integer,
              doc: """
              An integer that describes how many messages are allowed in the specified interval.
              """
            ],
            interval: [
              required: true,
              type: :pos_integer,
              doc: """
              An integer that describes the interval (in milliseconds)
              during which the number of allowed messages is allowed.
              If the producer produces more than `allowed_messages`
              in `interval`, only `allowed_messages` will be published until
              the end of `interval`, and then more messages will be published.
              """
            ]
          ]
        ]
      ]
    ],
    processors: [
      required: true,
      type: :non_empty_keyword_list,
      doc: """
      A keyword list of named processors where the key is an atom as identifier and
      the value is another keyword list of options.
      See ["Processors options"](#start_link/2-processors-options)
      section below. Currently only a single processor is allowed.
      """,
      subsection: """
      ### Processors options

      """,
      keys: [
        *: [
          type: :keyword_list,
          keys: [
            concurrency: [
              type: :pos_integer,
              doc: """
              The number of concurrent process that will
              be started by Broadway. Use this option to control the concurrency level
              of the processors. The default value is `System.schedulers_online() * 2`.
              """
            ],
            min_demand: [
              type: :non_neg_integer,
              doc: """
              Set the minimum demand of all processors
              stages. Default value is `5`.
              """
            ],
            max_demand: [
              type: :non_neg_integer,
              default: 10,
              doc: """
              Set the maximum demand of all processors
              stages. Default value is `10`.
              """
            ],
            partition_by: [
              type: {:fun, 1},
              doc: """
              Overrides the top-level `:partition_by`.
              """
            ],
            spawn_opt: [
              type: :keyword_list,
              doc: """
              Overrides the top-level `:spawn_opt`.
              """
            ],
            hibernate_after: [
              type: :pos_integer,
              doc: """
              Overrides the top-level `:hibernate_after`.
              """
            ]
          ]
        ]
      ]
    ],
    batchers: [
      default: [],
      type: :keyword_list,
      doc: """
      A keyword list of named batchers
      where the key is an atom as identifier and the value is another
      keyword list of options. See ["Batchers options"](#start_link/2-batchers-options)
      section below.
      """,
      subsection: """
      ### Batchers options

      """,
      keys: [
        *: [
          type: :keyword_list,
          keys: [
            concurrency: [
              type: :pos_integer,
              default: 1,
              doc: """
              The number of concurrent batch processors
              that will be started by Broadway. Use this option to control the
              concurrency level. Note that this only sets the numbers of batch
              processors for each batcher group, not the number of batchers.
              The number of batchers will always be one for each batcher key
              defined. The default value is `1`.
              """
            ],
            batch_size: [
              type: :pos_integer,
              default: 100,
              doc: """
              The size of the generated batches. Default value is `100`.
              """
            ],
            batch_timeout: [
              type: :pos_integer,
              default: 1000,
              doc: """
              The time, in milliseconds, that the batcher waits before flushing
              the list of messages. When this timeout is reached, a new batch
              is generated and sent downstream, no matter if the `:batch_size`
              has been reached or not. Default value is `1000` (1 second).
              """
            ],
            partition_by: [
              type: {:fun, 1},
              doc: """
              Optional. Overrides the top-level `:partition_by`.
              """
            ],
            spawn_opt: [
              type: :keyword_list,
              doc: """
              Overrides the top-level `:spawn_opt`.
              """
            ],
            hibernate_after: [
              type: :pos_integer,
              doc: """
              Overrides the top-level `:hibernate_after`.
              """
            ]
          ]
        ]
      ]
    ],
    partition_by: [
      type: {:fun, 1},
      doc: """
      A function that controls how data is
      partitioned across all processors and batchers. It receives a
      `Broadway.Message` and it must return a non-negative integer,
      starting with zero, that will be mapped to one of the existing
      processors. See ["Ordering and Partitioning"](#module-ordering-and-partitioning)
      in the module docs for more information.
      """
    ],
    spawn_opt: [
      type: :keyword_list,
      doc: """
      Low-level options given when starting a
      process. Applies to producers, processors, and batchers.
      See `erlang:spawn_opt/2` for more information.
      """
    ],
    hibernate_after: [
      type: :pos_integer,
      default: 15_000,
      doc: """
      If a process does not receive any message within this interval, it will hibernate,
      compacting memory. Applies to producers, processors, and batchers.
      Defaults to `15_000` (millisecond).
      """
    ]
  ]

  @definition NimbleOptions.new!(definition)

  def definition() do
    @definition
  end

  def validate_name(name) when is_atom(name), do: {:ok, name}

  def validate_name({:via, module, _term} = via) when is_atom(module), do: {:ok, via}

  def validate_name(name) do
    {:error,
     "expected :name to be an atom or a {:via, module, term} tuple, got: #{inspect(name)}"}
  end
end
