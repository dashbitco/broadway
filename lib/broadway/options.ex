defmodule Broadway.Options do
  @moduledoc false

  @basic_types [
    :any,
    :keyword_list,
    :non_empty_keyword_list,
    :atom,
    :non_neg_integer,
    :pos_integer,
    :mfa,
    :mod_arg
  ]

  def validate(opts, spec) do
    case validate_unknown_options(opts, spec) do
      :ok -> validate_options(spec, opts)
      error -> error
    end
  end

  defp validate_unknown_options(opts, spec) do
    valid_opts = Keyword.keys(spec)

    case Keyword.keys(opts) -- valid_opts do
      [] ->
        :ok

      keys ->
        {:error, "unknown options #{inspect(keys)}, valid options are: #{inspect(valid_opts)}"}
    end
  end

  defp validate_options(spec, opts) do
    case Enum.reduce_while(spec, opts, &reduce_options/2) do
      {:error, _} = result -> result
      result -> {:ok, result}
    end
  end

  defp reduce_options({key, spec_opts}, opts) do
    case validate_option(opts, key, spec_opts) do
      {:error, _} = result ->
        {:halt, result}

      {:ok, value} ->
        actual_key = spec_opts[:rename_to] || key
        {:cont, Keyword.update(opts, actual_key, value, fn _ -> value end)}

      :no_value ->
        if Keyword.has_key?(spec_opts, :default) do
          {:cont, Keyword.put(opts, key, spec_opts[:default])}
        else
          {:cont, opts}
        end
    end
  end

  defp validate_option(opts, key, spec) do
    with {:ok, value} <- validate_value(opts, key, spec),
         :ok <- validate_type(spec[:type], key, value) do
      if spec[:keys] do
        keys = normalize_keys(spec[:keys], value)
        validate(value, keys)
      else
        {:ok, value}
      end
    end
  end

  defp validate_value(opts, key, spec) do
    cond do
      Keyword.has_key?(opts, key) ->
        if message = Keyword.get(spec, :deprecated) do
          IO.warn("#{inspect(key)} is deprecated. " <> message)
        end

        {:ok, opts[key]}

      Keyword.get(spec, :required, false) ->
        {:error,
         "required option #{inspect(key)} not found, received options: " <>
           inspect(Keyword.keys(opts))}

      true ->
        :no_value
    end
  end

  defp validate_type(:non_neg_integer, key, value) when not is_integer(value) or value < 0 do
    {:error, "expected #{inspect(key)} to be a non negative integer, got: #{inspect(value)}"}
  end

  defp validate_type(:pos_integer, key, value) when not is_integer(value) or value < 1 do
    {:error, "expected #{inspect(key)} to be a positive integer, got: #{inspect(value)}"}
  end

  defp validate_type(:atom, key, value) when not is_atom(value) do
    {:error, "expected #{inspect(key)} to be an atom, got: #{inspect(value)}"}
  end

  defp validate_type(:keyword_list, key, value) do
    if keyword_list?(value) do
      :ok
    else
      {:error, "expected #{inspect(key)} to be a keyword list, got: #{inspect(value)}"}
    end
  end

  defp validate_type(:non_empty_keyword_list, key, value) do
    if keyword_list?(value) && value != [] do
      :ok
    else
      {:error, "expected #{inspect(key)} to be a non-empty keyword list, got: #{inspect(value)}"}
    end
  end

  defp validate_type(:mfa, _key, {m, f, args}) when is_atom(m) and is_atom(f) and is_list(args) do
    :ok
  end

  defp validate_type(:mfa, key, value) when not is_nil(value) do
    {:error, "expected #{inspect(key)} to be a tuple {Mod, Fun, Args}, got: #{inspect(value)}"}
  end

  defp validate_type(:mod_arg, _key, {m, _arg}) when is_atom(m) do
    :ok
  end

  defp validate_type(:mod_arg, key, value) do
    {:error, "expected #{inspect(key)} to be a tuple {Mod, Arg}, got: #{inspect(value)}"}
  end

  defp validate_type({:fun, arity}, key, value) when is_integer(arity) and arity >= 0 do
    expected = "expected #{inspect(key)} to be a function of arity #{arity}, "

    if is_function(value) do
      case :erlang.fun_info(value, :arity) do
        {:arity, ^arity} ->
          :ok

        {:arity, fun_arity} ->
          {:error, expected <> "got: function of arity #{inspect(fun_arity)}"}
      end
    else
      {:error, expected <> "got: #{inspect(value)}"}
    end
  end

  defp validate_type(nil, key, value) do
    validate_type(:any, key, value)
  end

  defp validate_type(type, _key, _value) when type in @basic_types do
    :ok
  end

  defp validate_type(type, _key, _value) do
    {:error, "invalid option type #{inspect(type)}, available types: #{available_types()}"}
  end

  defp tagged_tuple?({key, _value}) when is_atom(key), do: true
  defp tagged_tuple?(_), do: false

  defp keyword_list?(value) do
    is_list(value) && Enum.all?(value, &tagged_tuple?/1)
  end

  defp normalize_keys(keys, opts) do
    case keys[:*] do
      nil ->
        keys

      spec_opts ->
        Enum.map(opts, fn {k, _} -> {k, [type: :keyword_list, keys: spec_opts]} end)
    end
  end

  defp available_types() do
    types = Enum.map(@basic_types, &inspect/1) ++ ["{:fun, arity}"]
    Enum.join(types, ", ")
  end
end
