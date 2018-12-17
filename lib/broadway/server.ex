defmodule Broadway.Server do
  @moduledoc false
  use GenServer, shutdown: :infinity

  alias Broadway.{Producer, Processor, Batcher, Consumer}

  def start_link(module, context, opts) do
    GenServer.start_link(__MODULE__, {module, context, opts}, opts)
  end

  @impl true
  def init({module, context, opts}) do
    Process.flag(:trap_exit, true)
    {:ok, supervisor_pid} = start_supervisor(module, context, opts)
    {:ok, %{supervisor_pid: supervisor_pid}}
  end

  @impl true
  def handle_info({:EXIT, _, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %{supervisor_pid: supervisor_pid}) do
    ref = Process.monitor(supervisor_pid)
    Process.exit(supervisor_pid, :shutdown)

    receive do
      {:DOWN, ^ref, _, _, _} ->
        :ok
    end
  end

  defp start_supervisor(module, context, opts) do
    %{name: broadway_name} = config = init_config(module, context, opts)

    supervisor_name = Module.concat(broadway_name, "Supervisor")
    {producers_names, producers_specs} = build_producers_specs(config)
    {processors_names, processors_specs} = build_processors_specs(config, producers_names)

    children = [
      build_producer_supervisor_spec(producers_specs, broadway_name),
      build_processor_supervisor_spec(processors_specs, broadway_name),
      build_publisher_supervisor_spec(
        build_batchers_consumers_supervisors_specs(config, processors_names),
        broadway_name
      )
    ]

    Supervisor.start_link(children, name: supervisor_name, strategy: :one_for_one)
  end

  defp init_config(module, context, opts) do
    %{
      name: opts[:name],
      module: module,
      processors_config: opts[:processors],
      producers_config: opts[:producers],
      publishers_config: opts[:publishers],
      context: context
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
    args = producer_config[:arg]
    n_producers = producer_config[:stages]

    names =
      for index <- 1..n_producers do
        process_name(broadway_name, "Producer_#{key}", index, n_producers)
      end

    specs =
      for name <- names do
        args = [module: mod, args: args]
        opts = [name: name]

        %{
          start: {Producer, :start_link, [args, opts]},
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
      publishers_config: publishers_config
    } = config

    n_processors = processors_config[:stages]

    names =
      for index <- 1..n_processors do
        process_name(broadway_name, "Processor", index, n_processors)
      end

    specs =
      for name <- names do
        args = [
          publishers_config: publishers_config,
          processors_config: processors_config,
          module: module,
          context: context,
          producers: producers
        ]

        opts = [name: name]

        %{
          start: {Processor, :start_link, [args, opts]},
          id: name
        }
      end

    {names, specs}
  end

  defp build_batchers_consumers_supervisors_specs(config, processors) do
    %{
      name: broadway_name,
      module: module,
      context: context,
      publishers_config: publishers_config
    } = config

    for {key, _} = config <- publishers_config do
      {batcher, batcher_spec} = build_batcher_spec(broadway_name, config, processors)

      consumers_specs = build_consumers_specs(broadway_name, module, context, config, batcher)

      children = [
        batcher_spec,
        build_consumer_supervisor_spec(consumers_specs, broadway_name, key, batcher)
      ]

      build_batcher_consumer_supervisor_spec(children, broadway_name, key)
    end
  end

  defp build_batcher_spec(broadway_name, publisher_config, processors) do
    {key, options} = publisher_config
    name = process_name(broadway_name, "Batcher", key)

    args = [publisher_key: key, processors: processors] ++ options    
    opts = [name: name]

    spec = %{
      start: {Batcher, :start_link, [args, opts]},
      id: name
    }

    {name, spec}
  end

  defp build_consumers_specs(broadway_name, module, context, publisher_config, batcher) do
    {key, options} = publisher_config
    n_consumers = options[:stages]

    for index <- 1..n_consumers do
      args = [
        module: module,
        context: context,
        batcher: batcher
      ]

      name = process_name(broadway_name, "Consumer_#{key}", index, n_consumers)
      opts = [name: name]

      %{
        start: {Consumer, :start_link, [args, opts]},
        id: name
      }
    end
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
