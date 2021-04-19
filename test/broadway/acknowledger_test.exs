defmodule Broadway.AcknowledgerTest do
  use ExUnit.Case

  describe "crash_reason/3" do
    test "exceptions" do
      {kind, reason, stack} = kind_reason_stack(fn -> raise "oops" end)

      assert {%RuntimeError{message: "oops"}, [_ | _]} =
               Broadway.Acknowledger.crash_reason(kind, reason, stack)
    end

    test "exits" do
      {kind, reason, stack} = kind_reason_stack(fn -> exit(:fatal_error) end)

      assert {:fatal_error, [_ | _]} = Broadway.Acknowledger.crash_reason(kind, reason, stack)
    end

    test "throws" do
      {kind, reason, stack} = kind_reason_stack(fn -> throw(:basketball) end)

      assert {{:nocatch, :basketball}, [_ | _]} =
               Broadway.Acknowledger.crash_reason(kind, reason, stack)
    end

    test "Erlang errors" do
      {kind, reason, stack} = kind_reason_stack(fn -> :erlang.error(:boom) end)

      assert {%ErlangError{original: :boom}, [_ | _]} =
               Broadway.Acknowledger.crash_reason(kind, reason, stack)
    end
  end

  defp kind_reason_stack(fun) do
    fun.()
  catch
    kind, reason ->
      {kind, reason, __STACKTRACE__}
  end
end
