defmodule Broadway.Server do
  @moduledoc false
  @behaviour GenServer

  alias Broadway.{Producer, Processor, Batcher, Consumer, Terminator, RateLimiter}

  def start_link(module, opts) do
    GenServer.start_link(__MODULE__, {module, opts}, opts)
  end

  def producer_names(server) do
    GenServer.call(server, :producer_names)
  end

  def get_rate_limiter(server) do
    GenServer.call(server, :get_rate_limiter)
  end

  ## Callbacks

  @impl true
  def init({module, opts}) do
    Process.flag(:trap_exit, true)

    # We want to invoke this as early as possible otherwise the
    # stacktrace gets deeper and deeper in case of errors.
    {child_specs, opts} = prepare_for_start(module, opts)

    config = init_config(module, opts)
    {:ok, supervisor_pid} = start_supervisor(child_specs, config, opts)

    {:ok,
     %{
       supervisor_pid: supervisor_pid,
       terminator: config.terminator,
       name: opts[:name],
       producers_names: process_names(opts[:name], "Producer", config.producer_config),
       rate_limiter_name:
         config.producer_config[:rate_limiting] && RateLimiter.table_name(opts[:name])
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

  def handle_call(:get_rate_limiter, _from, %{rate_limiter_name: rate_limiter_name} = state) do
    if rate_limiter_name do
      {:reply, {:ok, rate_limiter_name}, state}
    else
      {:reply, {:error, :rate_limiting_not_enabled}, state}
    end
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

  defp prepare_for_start(module, opts) do
    {mod, _} = opts[:producer][:module]

    if Code.ensure_loaded?(mod) and function_exported?(mod, :prepare_for_start, 2) do
      mod.prepare_for_start(module, opts)
    else
      {[], opts}
    end
  end

  defp start_supervisor(child_specs, config, opts) do
    {producers_names, producers_specs} = build_producers_specs(config, opts)
    {processors_names, processors_specs} = build_processors_specs(config, producers_names)

    children =
      [
        build_rate_limiter_spec(config, producers_names),
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
      producer_config: opts[:producer],
      processors_config: opts[:processors],
      batchers_config: opts[:batchers],
      context: opts[:context],
      terminator: :"#{name_prefix(opts[:name])}.Terminator",
      max_restarts: opts[:max_restarts],
      max_seconds: opts[:max_seconds],
      shutdown: opts[:shutdown],
      resubscribe_interval: opts[:resubscribe_interval]
    }
  end

  defp start_options(name, config) do
    [name: name] ++ Keyword.take(config, [:spawn_opt, :hibernate_after])
  end

  defp build_rate_limiter_spec(config, producers_names) do
    %{name: broadway_name, producer_config: producer_config} = config

    opts = [
      name: broadway_name,
      rate_limiting: producer_config[:rate_limiting],
      producers_names: producers_names
    ]

    {RateLimiter, opts}
  end

  defp build_producers_specs(config, opts) do
    %{
      name: broadway_name,
      producer_config: producer_config,
      processors_config: processors_config,
      shutdown: shutdown
    } = config

    n_producers = producer_config[:concurrency]
    [{_, processor_config} | _other_processors] = processors_config

    # The partition of the producer depends on the processor, so we handle it here.
    dispatcher =
      case processor_config[:partition_by] do
        nil ->
          GenStage.DemandDispatcher

        func ->
          n_processors = processor_config[:concurrency]
          hash_func = fn msg -> {msg, rem(func.(msg), n_processors)} end
          {GenStage.PartitionDispatcher, partitions: 0..(n_processors - 1), hash: hash_func}
      end

    args = [broadway: opts, dispatcher: dispatcher] ++ producer_config

    names_and_specs =
      for index <- 0..(n_producers - 1) do
        name = process_name(broadway_name, "Producer", index)
        start_options = start_options(name, producer_config)

        spec = %{
          start: {Producer, :start_link, [args, index, start_options]},
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

    names = process_names(broadway_name, "Processor_#{key}", processor_config)

    # The partition of the processor depends on the next processor or the batcher,
    # so we handle it here.
    {type, dispatcher, batchers} =
      case Keyword.keys(batchers_config) do
        [] ->
          {:consumer, nil, :none}

        [_] = batchers ->
          {:producer_consumer, GenStage.DemandDispatcher, batchers}

        [_ | _] = batchers ->
          {:producer_consumer,
           {GenStage.PartitionDispatcher, partitions: batchers, hash: &{&1, &1.batcher}},
           batchers}
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
      batchers: batchers
    ]

    specs =
      for {name, index} <- Enum.with_index(names) do
        start_options = start_options(name, processor_config)
        args = [name: name, partition: index] ++ args

        %{
          start: {Processor, :start_link, [args, start_options]},
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
    name = process_name(config.name, "Batcher", key)

    args =
      [
        name: name,
        resubscribe: :never,
        terminator: terminator,
        batcher: key,
        partition: key,
        processors: processors,
        # Partitioning is handled inside the batcher since the batcher
        # needs to associate the partition with the batcher key.
        partition_by: options[:partition_by],
        concurrency: options[:concurrency]
      ] ++ options

    opts = [name: name]

    spec = %{
      start: {Batcher, :start_link, [args, opts]},
      id: name,
      shutdown: shutdown
    }

    {name, spec}
  end

  defp build_consumers_specs(config, {key, batcher_config}, batcher) do
    %{
      name: broadway_name,
      module: module,
      context: context,
      terminator: terminator,
      shutdown: shutdown
    } = config

    names = process_names(broadway_name, "Consumer_#{key}", batcher_config)

    args = [
      resubscribe: :never,
      terminator: terminator,
      module: module,
      context: context,
      batcher: batcher
    ]

    specs =
      for {name, index} <- Enum.with_index(names) do
        start_options = start_options(name, batcher_config)

        %{
          start: {Consumer, :start_link, [[name: name, partition: index] ++ args, start_options]},
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

    start_options = [name: name]

    %{
      start: {Terminator, :start_link, [args, start_options]},
      id: name,
      shutdown: shutdown
    }
  end

  defp name_prefix(prefix) do
    "#{prefix}.Broadway"
  end

  defp process_name(prefix, type, index) do
    :"#{name_prefix(prefix)}.#{type}_#{index}"
  end

  defp process_names(prefix, type, config) do
    for index <- 0..(config[:concurrency] - 1) do
      process_name(prefix, type, index)
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
