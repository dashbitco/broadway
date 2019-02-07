defmodule Broadway.Server do
  @moduledoc false
  use GenServer, shutdown: :infinity

  alias Broadway.{Producer, Processor, Batcher, Consumer, Terminator}

  def start_link(module, opts) do
    GenServer.start_link(__MODULE__, {module, opts}, opts)
  end

  @impl true
  def init({module, opts}) do
    Process.flag(:trap_exit, true)
    config = init_config(module, opts)
    {:ok, supervisor_pid} = start_supervisor(config)
    {:ok, %{supervisor_pid: supervisor_pid, terminator: config.terminator}}
  end

  @impl true
  def handle_info({:EXIT, supervisor_pid, reason}, %{supervisor_pid: supervisor_pid} = state) do
    {:stop, reason, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
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

  defp start_supervisor(config) do
    {producers_names, producers_specs} = build_producers_specs(config)
    {processors_names, processors_specs} = build_processors_specs(config, producers_names)

    {consumers_names, batchers_consumers_specs} =
      build_batchers_consumers_supervisors_specs(config, processors_names)

    children = [
      build_producer_supervisor_spec(config, producers_specs),
      build_processor_supervisor_spec(config, processors_specs),
      build_batcher_partition_supervisor_spec(config, batchers_consumers_specs),
      build_terminator_spec(config, producers_names, processors_names, consumers_names)
    ]

    supervisor_opts = [
      name: :"#{config.name}.Supervisor",
      max_restarts: config.max_restarts,
      max_seconds: config.max_seconds,
      strategy: :rest_for_one
    ]

    Supervisor.start_link(children, supervisor_opts)
  end

  defp init_config(module, opts) do
    %{
      name: opts[:name],
      module: module,
      processors_config: opts[:processors],
      producers_config: opts[:producers],
      publishers_config: opts[:publishers],
      context: opts[:context],
      terminator: :"#{opts[:name]}.Terminator",
      max_restarts: opts[:max_restarts],
      max_seconds: opts[:max_seconds],
      shutdown: opts[:shutdown],
      resubscribe_interval: opts[:resubscribe_interval]
    }
  end

  defp build_producers_specs(config) do
    %{
      name: broadway_name,
      producers_config: producers_config
    } = config

    [{key, producer_config} | other_producers] = producers_config

    if other_producers != [] do
      raise "Only one set of producers is allowed for now"
    end

    mod = producer_config[:module]
    arg = producer_config[:arg]
    n_producers = producer_config[:stages]

    names =
      for index <- 1..n_producers do
        process_name(broadway_name, "Producer_#{key}", index)
      end

    specs =
      for name <- names do
        opts = [name: name]

        %{
          start: {Producer, :start_link, [mod, arg, opts]},
          id: name
        }
      end

    {names, specs}
  end

  defp build_processors_specs(config, producers) do
    %{
      name: broadway_name,
      module: module,
      processors_config: processors_config,
      context: context,
      publishers_config: publishers_config,
      resubscribe_interval: resubscribe_interval,
      terminator: terminator
    } = config

    n_processors = processors_config[:stages]

    names =
      for index <- 1..n_processors do
        process_name(broadway_name, "Processor", index)
      end

    partitions = Keyword.keys(publishers_config)
    dispatcher = {GenStage.PartitionDispatcher, partitions: partitions, hash: &{&1, &1.publisher}}

    args = [
      type: :producer_consumer,
      resubscribe: resubscribe_interval,
      terminator: terminator,
      module: module,
      context: context,
      dispatcher: dispatcher,
      processors_config: processors_config,
      producers: producers
    ]

    specs =
      for name <- names do
        opts = [name: name]

        %{
          start: {Processor, :start_link, [args, opts]},
          id: name
        }
      end

    {names, specs}
  end

  defp build_batchers_consumers_supervisors_specs(config, processors) do
    names_and_specs =
      for {key, _} = publisher_config <- config.publishers_config do
        {batcher, batcher_spec} = build_batcher_spec(config, publisher_config, processors)

        {consumers_names, consumers_specs} =
          build_consumers_specs(config, publisher_config, batcher)

        children = [
          batcher_spec,
          build_consumer_supervisor_spec(config, consumers_specs, key)
        ]

        {consumers_names, build_batcher_consumer_supervisor_spec(config, children, key)}
      end

    {names, specs} = Enum.unzip(names_and_specs)
    {Enum.concat(names), specs}
  end

  defp build_batcher_spec(config, publisher_config, processors) do
    %{terminator: terminator} = config
    {key, options} = publisher_config
    name = process_name(config.name, "Batcher", key)

    args =
      [
        type: :producer_consumer,
        resubscribe: :never,
        terminator: terminator,
        publisher_key: key,
        processors: processors
      ] ++ options

    opts = [name: name]

    spec = %{
      start: {Batcher, :start_link, [args, opts]},
      id: name
    }

    {name, spec}
  end

  defp build_consumers_specs(config, publisher_config, batcher) do
    %{
      name: broadway_name,
      module: module,
      context: context,
      terminator: terminator
    } = config

    {key, options} = publisher_config
    n_consumers = options[:stages]

    names =
      for index <- 1..n_consumers do
        process_name(broadway_name, "Consumer_#{key}", index)
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
          id: name
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

  defp process_name(prefix, type, key) do
    :"#{prefix}.#{type}_#{key}"
  end

  defp build_producer_supervisor_spec(config, children) do
    name = :"#{config.name}.ProducerSupervisor"
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
    build_supervisor_spec(children, :"#{config.name}.ProcessorSupervisor",
      strategy: :one_for_all,
      max_restarts: 0
    )
  end

  defp build_batcher_partition_supervisor_spec(config, children) do
    children_count = length(children)

    build_supervisor_spec(children, :"#{config.name}.BatcherPartitionSupervisor",
      strategy: :one_for_one,
      max_restarts: 2 * children_count,
      max_seconds: children_count
    )
  end

  defp build_batcher_consumer_supervisor_spec(config, children, key) do
    build_supervisor_spec(children, :"#{config.name}.BatcherConsumerSupervisor_#{key}",
      strategy: :rest_for_one,
      max_restarts: 4,
      max_seconds: 2
    )
  end

  defp build_consumer_supervisor_spec(config, children, key) do
    build_supervisor_spec(children, :"#{config.name}.ConsumerSupervisor_#{key}",
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
