defmodule Broadway do
  use GenServer

  alias Broadway.{Processor, Batcher, Consumer, Message}

  @callback handle_message(message :: Message.t, context :: any) :: {:ok, message :: Message.t}
  @callback handle_batch(publisher :: atom, batch :: Batch.t, context :: any) :: {:ack, successful: [Message.t], failed: [Message.t]}

  defmodule State do
    defstruct name: nil,
              module: nil,
              processors_config: nil,
              producers_config: [],
              publishers_config: [],
              context: nil,
              supervisor: nil,
              producers: [],
              processors: [],
              batchers: [],
              consumers: []
  end

  def start_link(module, context, opts) do
    opts = Keyword.put(opts, :name, opts[:name] || broadway_name())
    GenServer.start_link(__MODULE__, {module, context, opts}, opts)
  end

  def init({module, context, opts}) do
    state =
      opts
      |> init_state(module, context)
      |> init_supervisor()
      |> init_producers()
      |> init_processors()
      |> init_batchers_and_consumers()

    {:ok, state}
  end

  defp init_state(opts, module, context) do
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

  def init_supervisor(%State{name: name} = state) do
    supervisor_name = Module.concat(name, "Supervisor")
    {:ok, supervisor} = Supervisor.start_link([], name: supervisor_name, strategy: :rest_for_one)

    %State{state | supervisor: supervisor}
  end

  defp init_producers(state) do
    %State{
      name: broadway_name,
      producers_config: producers_config,
      supervisor: supervisor
    } = state

    producers =
      for {[module: mod, arg: args], index} <- Enum.with_index(producers_config, 1) do
        mod_name = mod |> Module.split() |> Enum.join(".")
        name = process_name(broadway_name, mod_name, index, 1)
        opts = [name: name]

        spec =
          Supervisor.child_spec(
            %{start: {mod, :start_link, [args, opts]}},
            id: make_ref()
          )

        {:ok, _} = Supervisor.start_child(supervisor, spec)
        name
      end

    %State{state | producers: producers}
  end

  defp init_processors(state) do
    %State{
      name: broadway_name,
      module: module,
      processors_config: processors_config,
      context: context,
      supervisor: supervisor,
      publishers_config: publishers_config,
      producers: producers,
      supervisor: supervisor
    } = state

    n_processors = Keyword.fetch!(processors_config, :stages)

    processors =
      for index <- 1..n_processors do
        args = [
          publishers_config: publishers_config,
          module: module,
          context: context,
          producers: producers
        ]

        name = process_name(broadway_name, "Processor", index, n_processors)
        opts = [name: name]
        spec = Supervisor.child_spec({Processor, [args, opts]}, id: make_ref())
        {:ok, _} = Supervisor.start_child(supervisor, spec)
        name
      end

    %State{state | processors: processors}
  end

  defp init_batchers_and_consumers(state) do
    %State{
      name: broadway_name,
      module: module,
      context: context,
      supervisor: supervisor,
      publishers_config: publishers_config,
      supervisor: supervisor,
      processors: processors
    } = state

    stages =
      Enum.reduce(publishers_config, %{batchers: [], consumers: []}, fn config, acc ->
        {batcher, batcher_spec} = init_batcher(broadway_name, config, processors)
        {:ok, _} = Supervisor.start_child(supervisor, batcher_spec)

        {consumer, consumer_spec} =
          init_consumer(broadway_name, module, context, config, batcher)

        {:ok, _} = Supervisor.start_child(supervisor, consumer_spec)

        %{acc | batchers: [batcher | acc.batchers], consumers: [consumer | acc.consumers]}
      end)

    %State{
      state
      | batchers: Enum.reverse(stages.batchers),
        consumers: Enum.reverse(stages.consumers)
    }
  end

  defp init_batcher(broadway_name, publisher_config, processors) do
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

  defp init_consumer(broadway_name, module, context, publisher_config, batcher) do
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

  defp broadway_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
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
end
