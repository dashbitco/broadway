defmodule Broadway.Server do
  @moduledoc false
  @behaviour GenServer

  alias Broadway.{Producer, Processor, Batcher, Consumer, Terminator}

  def start_link(module, child_specs, opts) do
    GenServer.start_link(__MODULE__, {module, child_specs, opts}, opts)
  end

  def producer_names(server) do
    GenServer.call(server, :producer_names)
  end

  ## Callbacks

  @impl true
  def init({module, child_specs, opts}) do
    Process.flag(:trap_exit, true)
    config = init_config(module, opts)
    {:ok, supervisor_pid} = start_supervisor(child_specs, config)

    {:ok,
     %{
       supervisor_pid: supervisor_pid,
       terminator: config.terminator,
       name: opts[:name],
       producers_names: producer_names(name_prefix(opts[:name]), config.producer_config)
     }}
  end

  @impl true
  def handle_info({:EXIT, supervisor_pid, reason}, %{supervisor_pid: supervisor_pid} = state) do
    {:stop, reason, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  @impl true
  def handle_call(:producer_names, _from, state) do
    {:reply, state.producers_names, state}
  end

  @impl true
  def terminate(reason, %{supervisor_pid: supervisor_pid, terminator: terminator}) do
    Broadway.Terminator.trap_exit(terminator)
    ref = Process.monitor(supervisor_pid)
    Process.exit(supervisor_pid, reason_to_signal(reason))

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end

  defp reason_to_signal(:killed), do: :kill
  defp reason_to_signal(other), do: other

  defp start_supervisor(child_specs, config) do
    {producers_names, producers_specs} = build_producers_specs(config)
    {processors_names, processors_specs} = build_processors_specs(config, producers_names)

    children =
      [
        build_producer_supervisor_spec(config, producers_specs),
        build_processor_supervisor_spec(config, processors_specs)
      ] ++
        build_batcher_supervisor_and_terminator_specs(config, producers_names, processors_names)

    supervisor_opts = [
      name: :"#{name_prefix(config.name)}.Supervisor",
      max_restarts: config.max_restarts,
      max_seconds: config.max_seconds,
      strategy: :rest_for_one
    ]

    Supervisor.start_link(child_specs ++ children, supervisor_opts)
  end

  defp init_config(module, opts) do
    %{
      name: opts[:name],
      module: module,
      processors_config: opts[:processors],
      producer_config: opts[:producer],
      batchers_config: opts[:batchers],
      context: opts[:context],
      terminator: :"#{name_prefix(opts[:name])}.Terminator",
      max_restarts: opts[:max_restarts],
      max_seconds: opts[:max_seconds],
      shutdown: opts[:shutdown],
      resubscribe_interval: opts[:resubscribe_interval]
    }
  end

  defp build_producers_specs(config) do
    %{
      name: broadway_name,
      producer_config: producer_config,
      shutdown: shutdown
    } = config

    n_producers = producer_config[:stages]

    names_and_specs =
      for index <- 1..n_producers do
        name = producer_name(name_prefix(broadway_name), index)
        opts = [name: name]

        spec = %{
          start: {Producer, :start_link, [producer_config, index, opts]},
          id: name,
          shutdown: shutdown
        }

        {name, spec}
      end

    # We want to return {names, specs} here.
    Enum.unzip(names_and_specs)
  end

  defp build_processors_specs(config, producers) do
    %{
      name: broadway_name,
      module: module,
      processors_config: processors_config,
      context: context,
      batchers_config: batchers_config,
      resubscribe_interval: resubscribe_interval,
      terminator: terminator,
      shutdown: shutdown
    } = config

    [{key, processor_config} | other_processors] = processors_config

    if other_processors != [] do
      raise "Only one set of processors is allowed for now"
    end

    n_processors = processor_config[:stages]

    names =
      for index <- 1..n_processors do
        process_name(name_prefix(broadway_name), "Processor_#{key}", index)
      end

    partitions = Keyword.keys(batchers_config)

    {type, dispatcher} =
      case partitions do
        [] ->
          {:consumer, nil}

        [_] ->
          {:producer_consumer, GenStage.DemandDispatcher}

        [_ | _] ->
          {:producer_consumer,
           {GenStage.PartitionDispatcher, partitions: partitions, hash: &{&1, &1.batcher}}}
      end

    args = [
      type: type,
      resubscribe: resubscribe_interval,
      terminator: terminator,
      module: module,
      context: context,
      dispatcher: dispatcher,
      processor_key: key,
      processor_config: processor_config,
      producers: producers,
      partitions: partitions
    ]

    specs =
      for name <- names do
        opts = [name: name]

        %{
          start: {Processor, :start_link, [args, opts]},
          id: name,
          shutdown: shutdown
        }
      end

    {names, specs}
  end

  defp build_batcher_supervisor_and_terminator_specs(config, producers_names, processors_names) do
    if config[:batchers_config] == [] do
      [build_terminator_spec(config, producers_names, processors_names, processors_names)]
    else
      {consumers_names, batchers_consumers_specs} =
        build_batchers_consumers_supervisors_specs(config, processors_names)

      [
        build_batcher_partition_supervisor_spec(config, batchers_consumers_specs),
        build_terminator_spec(config, producers_names, processors_names, consumers_names)
      ]
    end
  end

  defp build_batchers_consumers_supervisors_specs(config, processors) do
    names_and_specs =
      for {key, _} = batcher_config <- config.batchers_config do
        {batcher, batcher_spec} = build_batcher_spec(config, batcher_config, processors)

        {consumers_names, consumers_specs} =
          build_consumers_specs(config, batcher_config, batcher)

        children = [
          batcher_spec,
          build_consumer_supervisor_spec(config, consumers_specs, key)
        ]

        {consumers_names, build_batcher_consumer_supervisor_spec(config, children, key)}
      end

    {names, specs} = Enum.unzip(names_and_specs)
    {Enum.concat(names), specs}
  end

  defp build_batcher_spec(config, batcher_config, processors) do
    %{terminator: terminator, shutdown: shutdown} = config
    {key, options} = batcher_config
    name = process_name(name_prefix(config.name), "Batcher", key)

    args =
      [
        type: :producer_consumer,
        resubscribe: :never,
        terminator: terminator,
        batcher: key,
        processors: processors
      ] ++ options

    opts = [name: name]

    spec = %{
      start: {Batcher, :start_link, [args, opts]},
      id: name,
      shutdown: shutdown
    }

    {name, spec}
  end

  defp build_consumers_specs(config, batcher_config, batcher) do
    %{
      name: broadway_name,
      module: module,
      context: context,
      terminator: terminator,
      shutdown: shutdown
    } = config

    {key, options} = batcher_config
    n_consumers = options[:stages]

    names =
      for index <- 1..n_consumers do
        process_name(name_prefix(broadway_name), "Consumer_#{key}", index)
      end

    args = [
      type: :consumer,
      resubscribe: :never,
      terminator: terminator,
      module: module,
      context: context,
      batcher: batcher
    ]

    specs =
      for name <- names do
        opts = [name: name]

        %{
          start: {Consumer, :start_link, [args, opts]},
          id: name,
          shutdown: shutdown
        }
      end

    {names, specs}
  end

  defp build_terminator_spec(config, producers, first, last) do
    %{
      terminator: name,
      shutdown: shutdown
    } = config

    args = [
      producers: producers,
      first: first,
      last: last
    ]

    opts = [name: name]

    %{
      start: {Terminator, :start_link, [args, opts]},
      id: name,
      shutdown: shutdown
    }
  end

  defp name_prefix(name) do
    "#{name}.Broadway"
  end

  defp process_name(prefix, type, key) do
    :"#{prefix}.#{type}_#{key}"
  end

  defp producer_name(broadway_name, index) do
    process_name(broadway_name, "Producer", index)
  end

  defp producer_names(broadway_name, producer_config) do
    for index <- 1..producer_config[:stages] do
      producer_name(broadway_name, index)
    end
  end

  defp build_producer_supervisor_spec(config, children) do
    name = :"#{name_prefix(config.name)}.ProducerSupervisor"
    children_count = length(children)

    # TODO: Allow max_restarts and max_seconds as configuration
    # options as well as shutdown and restart for each child.
    build_supervisor_spec(children, name,
      strategy: :one_for_one,
      max_restarts: 2 * children_count,
      max_seconds: children_count
    )
  end

  defp build_processor_supervisor_spec(config, children) do
    build_supervisor_spec(
      children,
      :"#{name_prefix(config.name)}.ProcessorSupervisor",
      strategy: :one_for_all,
      max_restarts: 0
    )
  end

  defp build_batcher_partition_supervisor_spec(config, children) do
    children_count = length(children)

    build_supervisor_spec(
      children,
      :"#{name_prefix(config.name)}.BatcherPartitionSupervisor",
      strategy: :one_for_one,
      max_restarts: 2 * children_count,
      max_seconds: children_count
    )
  end

  defp build_batcher_consumer_supervisor_spec(config, children, key) do
    build_supervisor_spec(
      children,
      :"#{name_prefix(config.name)}.BatcherConsumerSupervisor_#{key}",
      strategy: :rest_for_one,
      max_restarts: 4,
      max_seconds: 2
    )
  end

  defp build_consumer_supervisor_spec(config, children, key) do
    build_supervisor_spec(
      children,
      :"#{name_prefix(config.name)}.ConsumerSupervisor_#{key}",
      strategy: :one_for_all,
      max_restarts: 0
    )
  end

  defp build_supervisor_spec(children, name, opts) do
    %{
      id: make_ref(),
      start: {Supervisor, :start_link, [children, [name: name] ++ opts]},
      type: :supervisor
    }
  end
end
