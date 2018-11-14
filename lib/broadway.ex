defmodule Broadway do
  use GenServer

  alias Broadway.{Processor, Batcher, Consumer, Message}

  @callback handle_message(message :: Message.t(), context :: any) ::
              {:ok, message :: Message.t()}
  @callback handle_batch(publisher :: atom, batch :: Batch.t(), context :: any) ::
              {:ack, successful: [Message.t()], failed: [Message.t()]}

  defmodule State do
    defstruct name: nil,
              module: nil,
              processors_config: [],
              producers_config: [],
              publishers_config: [],
              context: nil,
              supervisor: nil
  end

  def start_link(module, context, opts) do
    GenServer.start_link(__MODULE__, {module, context, opts}, opts)
  end

  def init({module, context, opts}) do
    state = init_state(module, context, opts)
    {:ok, supervisor} = start_supervisor(state)

    {:ok, %{state | supervisor: supervisor}}
  end

  defp init_state(module, context, opts) do
    broadway_name = Keyword.fetch!(opts, :name)
    producers_config = Keyword.fetch!(opts, :producers)
    publishers_config = Keyword.get(opts, :publishers) |> normalize_publishers_config()
    processors_config = Keyword.get(opts, :processors) |> normalize_processors_config()

    %State{
      name: broadway_name,
      module: module,
      processors_config: processors_config,
      producers_config: producers_config,
      publishers_config: publishers_config,
      context: context
    }
  end

  def start_supervisor(%State{name: broadway_name} = state) do
    supervisor_name = Module.concat(broadway_name, "Supervisor")
    {producers_names, producers_specs} = build_producers_specs(state)
    {processors_names, processors_specs} = build_processors_specs(state, producers_names)

    children = [
      build_producer_supervisor_spec(producers_specs, broadway_name),
      build_processor_supervisor_spec(processors_specs, broadway_name),
      build_publisher_supervisor_spec(
        build_batchers_consumers_supervisors_specs(state, processors_names),
        broadway_name
      )
    ]

    Supervisor.start_link(children, name: supervisor_name, strategy: :one_for_one)
  end

  defp build_producers_specs(state) do
    %State{
      name: broadway_name,
      producers_config: producers_config
    } = state

    init_acc = %{names: [], specs: []}

    producers =
      producers_config
      |> Enum.with_index(1)
      |> Enum.reduce(init_acc, fn {[module: mod, arg: args], index}, acc ->
        mod_name = mod |> Module.split() |> Enum.join(".")
        name = process_name(broadway_name, mod_name, index, 1)
        opts = [name: name]

        spec =
          Supervisor.child_spec(
            %{start: {mod, :start_link, [args, opts]}},
            id: make_ref()
          )

        %{acc | names: [name | acc.names], specs: [spec | acc.specs]}
      end)

    {Enum.reverse(producers.names), Enum.reverse(producers.specs)}
  end

  defp build_processors_specs(state, producers) do
    %State{
      name: broadway_name,
      module: module,
      processors_config: processors_config,
      context: context,
      supervisor: supervisor,
      publishers_config: publishers_config,
      supervisor: supervisor
    } = state

    n_processors = Keyword.fetch!(processors_config, :stages)

    init_acc = %{names: [], specs: []}

    processors =
      Enum.reduce(1..n_processors, init_acc, fn index, acc ->
        args = [
          publishers_config: publishers_config,
          module: module,
          context: context,
          producers: producers
        ]

        name = process_name(broadway_name, "Processor", index, n_processors)
        opts = [name: name]
        spec = Supervisor.child_spec({Processor, [args, opts]}, id: make_ref())

        %{acc | names: [name | acc.names], specs: [spec | acc.specs]}
      end)

    {Enum.reverse(processors.names), Enum.reverse(processors.specs)}
  end

  defp build_batchers_consumers_supervisors_specs(state, processors) do
    %State{
      name: broadway_name,
      module: module,
      context: context,
      publishers_config: publishers_config
    } = state

    for {key, _} = config <- publishers_config do
      {batcher, batcher_spec} = build_batcher_spec(broadway_name, config, processors)

      {_consumer, consumer_spec} =
        build_consumer_spec(broadway_name, module, context, config, batcher)

      children = [
        batcher_spec,
        build_consumer_supervisor_spec([consumer_spec], broadway_name, key, batcher)
      ]

      build_batcher_consumer_supervisor_spec(children, broadway_name, key)
    end
  end

  defp build_batcher_spec(broadway_name, publisher_config, processors) do
    {key, options} = publisher_config
    batcher = process_name(broadway_name, "Batcher", key)
    opts = [name: batcher]

    spec =
      Supervisor.child_spec(
        {Batcher, [options ++ [publisher_key: key, processors: processors], opts]},
        id: make_ref()
      )

    {batcher, spec}
  end

  defp build_consumer_spec(broadway_name, module, context, publisher_config, batcher) do
    {key, _options} = publisher_config
    consumer = process_name(broadway_name, "Consumer", key)
    opts = [name: consumer]

    spec =
      Supervisor.child_spec(
        {Consumer, [[module: module, context: context, batcher: batcher], opts]},
        id: make_ref()
      )

    {consumer, spec}
  end

  defp normalize_processors_config(nil) do
    [stages: :erlang.system_info(:schedulers_online) * 2]
  end

  defp normalize_processors_config(config) do
    config
  end

  defp normalize_publishers_config(nil) do
    [{:default, []}]
  end

  defp normalize_publishers_config(publishers) do
    Enum.map(publishers, fn
      publisher when is_atom(publisher) -> {publisher, []}
      publisher -> publisher
    end)
  end

  defp process_name(prefix, type, key) do
    :"#{prefix}.#{type}_#{key}"
  end

  defp process_name(prefix, type, index, max) do
    index
    |> to_string()
    |> String.pad_leading(String.length("#{max}"), "0")
    |> (&:"#{prefix}.#{type}_#{&1}").()
  end

  defp build_producer_supervisor_spec(children, prefix) do
    build_supervisor_spec(children, :"#{prefix}.ProducerSupervisor")
  end

  defp build_processor_supervisor_spec(children, prefix) do
    build_supervisor_spec(children, :"#{prefix}.ProcessorSupervisor")
  end

  defp build_publisher_supervisor_spec(children, prefix) do
    build_supervisor_spec(children, :"#{prefix}.PublisherSupervisor")
  end

  defp build_batcher_consumer_supervisor_spec(children, prefix, key) do
    build_supervisor_spec(children, :"#{prefix}.BatcherConsumerSupervisor_#{key}")
  end

  defp build_consumer_supervisor_spec(children, prefix, key, batcher) do
    build_supervisor_spec(children, :"#{prefix}.ConsumerSupervisor_#{key}", batcher: batcher)
  end

  defp build_supervisor_spec(children, name, extra_opts \\ []) do
    opts = [name: name, strategy: :one_for_one]

    %{
      id: make_ref(),
      start: {Supervisor, :start_link, [children, opts ++ extra_opts]},
      type: :supervisor
    }
  end
end
