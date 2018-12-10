defmodule Broadway.Options do
  @moduledoc false

  @types [:any, :keyword_list, :atom, :non_neg_integer, :pos_integer]

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
    Enum.reduce_while(spec, opts, fn {key, spec_opts}, parent_opts ->
      case validate_option(parent_opts, key, spec_opts) do
        {:error, _} = result ->
          {:halt, result}

        new_value ->
          {:cont, Keyword.put(parent_opts, key, new_value)}
      end
    end)
  end

  defp validate_option(opts, key, spec) do
    with value <- get_value_or_default(opts, key, spec),
         :ok <- validate_required(opts, key, spec),
         :ok <- validate_type(spec[:type], key, value) do
      if spec[:keys] do
        keys = normalize_keys(spec[:keys], value)
        validate(opts[key], keys)
      else
        value
      end
    end
  end

  defp get_value_or_default(opts, key, spec) do
    if opts[key] == nil && spec[:default] do
      spec[:default]
    else
      opts[key]
    end
  end

  defp validate_required(opts, key, spec) do
    if Keyword.get(spec, :required) && !Keyword.has_key?(opts, key) do
      {:error,
       "required option #{inspect(key)} not found, received options: #{
         inspect(Keyword.keys(opts))
       }"}
    else
      :ok
    end
  end

  defp validate_type(:non_neg_integer, key, value) when not is_integer(value) or value < 0 do
    {:error, "expected #{inspect(key)} to be a non negative integer, got: #{inspect(value)}"}
  end

  defp validate_type(:pos_integer, key, value) when not is_integer(value) or value < 1 do
    {:error, "expected #{inspect(key)} to be a positive integer, got: #{inspect(value)}"}
  end

  defp validate_type(:atom, key, value) when not is_atom(value) do
    {:error, "expected #{inspect(key)} to be a atom, got: #{inspect(value)}"}
  end

  defp validate_type(:keyword_list, key, value) do
    if is_list(value) && Enum.all?(value, &tagged_tuple?/1) do
      :ok
    else
      {:error, "expected #{inspect(key)} to be a keyword list, got: #{inspect(value)}"}
    end
  end

  defp validate_type(nil, key, value) do
    validate_type(:any, key, value)
  end

  defp validate_type(type, _key, _value) when type in @types do
    :ok
  end

  defp validate_type(type, _key, _value) do
    {:error, "invalid option type #{inspect(type)}, available types: #{inspect(@types)}"}
  end

  defp tagged_tuple?({key, _value}) when is_atom(key), do: true
  defp tagged_tuple?(_), do: false

  defp normalize_keys(keys, opts) do
    case keys[:*] do
      nil ->
        keys

      spec_opts ->
        opts
        |> Keyword.keys()
        |> Enum.map(fn k -> {k, [type: :keyword_list, keys: spec_opts]} end)
    end
  end
end
