defmodule Broadway.Topology do
  @moduledoc false
  @behaviour GenServer

  alias Broadway.Topology.{
    ProducerStage,
    ProcessorStage,
    BatcherStage,
    BatchProcessorStage,
    Terminator,
    RateLimiter
  }

  alias Broadway.ConfigStorage

  defstruct [:context, :topology, :producer_names, :batchers_names, :rate_limiter_name]

  def start_link(module, opts) do
    GenServer.start_link(__MODULE__, {module, opts}, opts)
  end

  def producer_names(server) do
    config(server).producer_names
  end

  def get_rate_limiter(server) do
    if name = config(server).rate_limiter_name do
      {:ok, name}
    else
      {:error, :rate_limiting_not_enabled}
    end
  end

  def topology(server) do
    config(server).topology
  end

  defp config(server) do
    config_storage = ConfigStorage.get_module()

    config_storage.get(server) ||
      exit({:noproc, {__MODULE__, :config, [server]}})
  end

  ## Callbacks

  @impl true
  def init({module, opts}) do
    Process.flag(:trap_exit, true)
    config_storage = ConfigStorage.get_module()

    # We want to invoke this as early as possible otherwise the
    # stacktrace gets deeper and deeper in case of errors.
    {child_specs, opts} = prepare_for_start(module, opts)

    config = init_config(module, opts)
    {:ok, supervisor_pid} = start_supervisor(child_specs, config, opts)

    emit_init_event(opts, supervisor_pid)

    config_storage.put(config.name, %__MODULE__{
      context: config.context,
      topology: build_topology_details(config),
      producer_names: process_names(config, "Producer", config.producer_config),
      batchers_names:
        Enum.map(config.batchers_config, &process_name(config, "Batcher", elem(&1, 0))),
      rate_limiter_name: config.rate_limiter
    })

    {:ok,
     %{
       supervisor_pid: supervisor_pid,
       terminator: config.terminator,
       name: config.name
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
  def terminate(reason, %{name: name, supervisor_pid: supervisor_pid, terminator: terminator}) do
    Broadway.Topology.Terminator.trap_exit(terminator)
    ref = Process.monitor(supervisor_pid)
    Process.exit(supervisor_pid, reason_to_signal(reason))

    receive do
      {:DOWN, ^ref, _, _, _} ->
        config_storage = ConfigStorage.get_module()
        config_storage.delete(name)
        :ok
    end

    :ok
  end

  defp reason_to_signal(:killed), do: :kill
  defp reason_to_signal(other), do: other

  defp prepare_for_start(module, opts) do
    {producer_mod, _producer_opts} = opts[:producer][:module]

    if Code.ensure_loaded?(producer_mod) and
         function_exported?(producer_mod, :prepare_for_start, 2) do
      case producer_mod.prepare_for_start(module, opts) do
        {child_specs, opts} when is_list(child_specs) ->
          {child_specs, NimbleOptions.validate!(opts, Broadway.Options.definition())}

        other ->
          raise ArgumentError,
                "expected #{Exception.format_mfa(producer_mod, :prepare_for_start, 2)} " <>
                  "to return {child_specs, options}, got: #{inspect(other)}"
      end
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
        build_batchers_supervisor_and_terminator_specs(config, producers_names, processors_names)

    supervisor_opts = [
      name: process_name(config, "Supervisor"),
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
      processors_config: init_processors_config(opts[:processors]),
      batchers_config: opts[:batchers],
      context: opts[:context],
      max_restarts: opts[:max_restarts],
      max_seconds: opts[:max_seconds],
      shutdown: opts[:shutdown],
      resubscribe_interval: opts[:resubscribe_interval],
      terminator: nil,
      rate_limiter: nil
    }
    |> put_terminator()
    |> put_rate_limiter(opts)
  end

  defp put_terminator(config) do
    Map.put(config, :terminator, process_name(config, "Terminator"))
  end

  defp put_rate_limiter(config, opts) do
    if opts[:producer][:rate_limiting] do
      Map.put(config, :rate_limiter, process_name(config, "RateLimiter"))
    else
      config
    end
  end

  defp init_processors_config(config) do
    Enum.map(config, fn {key, opts} ->
      {key, Keyword.put_new(opts, :concurrency, System.schedulers_online() * 2)}
    end)
  end

  defp emit_init_event(user_config, supervisor_pid) do
    measurements = %{system_time: System.monotonic_time()}

    metadata = %{
      config: user_config,
      supervisor_pid: supervisor_pid
    }

    :telemetry.execute([:broadway, :topology, :init], measurements, metadata)
  end

  defp start_options(name, config) do
    [name: name] ++ Keyword.take(config, [:spawn_opt, :hibernate_after])
  end

  defp build_rate_limiter_spec(config, producers_names) do
    %{producer_config: producer_config} = config

    opts = [
      name: process_name(config, "RateLimiter"),
      rate_limiting: producer_config[:rate_limiting],
      producers_names: producers_names
    ]

    {RateLimiter, opts}
  end

  defp build_producers_specs(config, opts) do
    %{
      producer_config: producer_config,
      processors_config: processors_config,
      shutdown: shutdown,
      rate_limiter: rate_limiter
    } = config

    n_producers = producer_config[:concurrency]
    [{_, processor_config} | _other_processors] = processors_config

    # The partition of the producer depends on the processor, so we handle it here.
    dispatcher =
      case processor_config[:partition_by] do
        nil ->
          {GenStage.DemandDispatcher, shuffle_demands_on_first_dispatch: true}

        func ->
          n_processors = processor_config[:concurrency]
          hash_func = fn msg -> {msg, rem(func.(msg), n_processors)} end
          {GenStage.PartitionDispatcher, partitions: 0..(n_processors - 1), hash: hash_func}
      end

    args = [broadway: opts, dispatcher: dispatcher, rate_limiter: rate_limiter] ++ producer_config

    names_and_specs =
      for index <- 0..(n_producers - 1) do
        name = process_name(config, "Producer", index)
        start_options = start_options(name, producer_config)

        spec = %{
          start: {ProducerStage, :start_link, [args, index, start_options]},
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
      name: topology_name,
      module: module,
      processors_config: processors_config,
      context: context,
      batchers_config: batchers_config,
      resubscribe_interval: resubscribe_interval,
      terminator: terminator,
      shutdown: shutdown,
      producer_config: producer_config
    } = config

    [{key, processor_config} | other_processors] = processors_config

    if other_processors != [] do
      raise "Only one set of processors is allowed for now"
    end

    names = process_names(config, "Processor_#{key}", processor_config)

    # The partition of the processor depends on the next processor or the batcher,
    # so we handle it here.
    {type, dispatcher, batchers} =
      case Keyword.keys(batchers_config) do
        [] ->
          {:consumer, nil, :none}

        [_] = batchers ->
          {:producer_consumer,
           {GenStage.DemandDispatcher, shuffle_demands_on_first_dispatch: true}, batchers}

        [_ | _] = batchers ->
          {:producer_consumer,
           {GenStage.PartitionDispatcher, partitions: batchers, hash: &{&1, &1.batcher}},
           batchers}
      end

    args = [
      topology_name: topology_name,
      type: type,
      resubscribe: resubscribe_interval,
      terminator: terminator,
      module: module,
      context: context,
      dispatcher: dispatcher,
      processor_key: key,
      processor_config: processor_config,
      producers: producers,
      producer: producer_config[:module],
      batchers: batchers
    ]

    specs =
      for {name, index} <- Enum.with_index(names) do
        start_options = start_options(name, processor_config)
        args = [name: name, partition: index] ++ args

        %{
          start: {ProcessorStage, :start_link, [args, start_options]},
          id: name,
          shutdown: shutdown
        }
      end

    {names, specs}
  end

  defp build_batchers_supervisor_and_terminator_specs(config, producers_names, processors_names) do
    if config.batchers_config == [] do
      [build_terminator_spec(config, producers_names, processors_names, processors_names)]
    else
      {batch_processors_names, batcher_supervisors_specs} =
        build_batcher_supervisors_specs(config, processors_names)

      [
        build_batchers_supervisor_spec(config, batcher_supervisors_specs),
        build_terminator_spec(config, producers_names, processors_names, batch_processors_names)
      ]
    end
  end

  defp build_batcher_supervisors_specs(config, processors) do
    names_and_specs =
      for {key, _} = batcher_config <- config.batchers_config do
        {batcher, batcher_spec} = build_batcher_spec(config, batcher_config, processors)

        {consumers_names, consumers_specs} =
          build_batch_processors_specs(config, batcher_config, batcher)

        children = [
          batcher_spec,
          build_batch_processor_supervisor_spec(config, consumers_specs, key)
        ]

        {consumers_names, build_batcher_supervisor_spec(config, children, key)}
      end

    {names, specs} = Enum.unzip(names_and_specs)
    {Enum.concat(names), specs}
  end

  defp build_batcher_spec(config, batcher_config, processors) do
    %{terminator: terminator, shutdown: shutdown} = config
    {key, options} = batcher_config
    name = process_name(config, "Batcher", key)

    args =
      [
        topology_name: config.name,
        name: name,
        resubscribe: :never,
        terminator: terminator,
        batcher: key,
        partition: key,
        processors: processors,
        context: config[:context],
        # Partitioning is handled inside the batcher since the batcher
        # needs to associate the partition with the batcher key.
        partition_by: options[:partition_by],
        concurrency: options[:concurrency]
      ] ++ options

    opts = [name: name]

    spec = %{
      start: {BatcherStage, :start_link, [args, opts]},
      id: name,
      shutdown: shutdown
    }

    {name, spec}
  end

  defp build_batch_processors_specs(config, {key, batcher_config}, batcher) do
    %{
      name: broadway_name,
      module: module,
      context: context,
      terminator: terminator,
      shutdown: shutdown,
      producer_config: producer_config
    } = config

    names = process_names(config, "BatchProcessor_#{key}", batcher_config)

    args = [
      topology_name: broadway_name,
      resubscribe: :never,
      terminator: terminator,
      module: module,
      context: context,
      batcher: batcher,
      producer: producer_config[:module]
    ]

    specs =
      for {name, index} <- Enum.with_index(names) do
        start_options = start_options(name, batcher_config)

        %{
          start:
            {BatchProcessorStage, :start_link,
             [[name: name, partition: index] ++ args, start_options]},
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

  defp build_topology_details(config) do
    [
      producers: [
        %{
          name: process_name(config, "Producer"),
          concurrency: config.producer_config[:concurrency]
        }
      ],
      processors:
        Enum.map(config.processors_config, fn {name, processor_config} ->
          %{
            name: process_name(config, "Processor", name),
            processor_key: name,
            concurrency: processor_config[:concurrency]
          }
        end),
      batchers:
        Enum.map(config.batchers_config, fn {name, batcher_config} ->
          %{
            batcher_name: process_name(config, "Batcher", name),
            batcher_key: name,
            name: process_name(config, "BatchProcessor", name),
            concurrency: batcher_config[:concurrency]
          }
        end)
    ]
  end

  defp process_name(config, base_name, suffix) do
    process_name(config, "#{base_name}_#{suffix}")
  end

  defp process_name(%{module: module, name: broadway_name} = _config, base_name) do
    if function_exported?(module, :process_name, 2) do
      module.process_name(broadway_name, base_name)
    else
      default_process_name(broadway_name, base_name)
    end
  end

  defp default_process_name(broadway_name, base_name) when is_atom(broadway_name) do
    :"#{broadway_name}.Broadway.#{base_name}"
  end

  defp default_process_name(broadway_name, _base_name) do
    raise ArgumentError, """
    expected Broadway to be started with an atom :name, got: #{inspect(broadway_name)}

    If starting Broadway with a :name that is not an atom, you must define the \
    process_name/2 callback in the module which calls "use Broadway" (see the documentation).
    """
  end

  defp process_names(config, type, processor_config) do
    for index <- 0..(processor_config[:concurrency] - 1) do
      process_name(config, type, index)
    end
  end

  defp build_producer_supervisor_spec(config, children) do
    name = process_name(config, "ProducerSupervisor")
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
      process_name(config, "ProcessorSupervisor"),
      strategy: :one_for_all,
      max_restarts: 0
    )
  end

  defp build_batchers_supervisor_spec(config, children) do
    children_count = length(children)

    build_supervisor_spec(
      children,
      process_name(config, "BatchersSupervisor"),
      strategy: :one_for_one,
      max_restarts: 2 * children_count,
      max_seconds: children_count
    )
  end

  defp build_batcher_supervisor_spec(config, children, key) do
    build_supervisor_spec(
      children,
      process_name(config, "BatcherSupervisor", key),
      strategy: :rest_for_one,
      max_restarts: 4,
      max_seconds: 2
    )
  end

  defp build_batch_processor_supervisor_spec(config, children, key) do
    build_supervisor_spec(
      children,
      process_name(config, "BatchProcessorSupervisor", key),
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
