defmodule Broadway.Process do
  @moduledoc false

  # TODO: Remove this module once we require Elixir 1.17+.
  # Process.set_label/1 was added in Elixir 1.17.0.

  if function_exported?(Process, :set_label, 1) do
    def set_label(label) do
      Process.set_label(label)
    end

    def labels_supported?, do: true
  else
    def set_label(_label) do
      :ok
    end

    def labels_supported?, do: false
  end
end
