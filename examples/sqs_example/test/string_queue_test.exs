defmodule StringQueueTest do
  use ExUnit.Case, async: true
  doctest BroadwaySQSExample.String

  test "Acks work" do
    ref = Broadway.test_messages(BroadwaySQSExample.String, ["1", "2", "3"])
    assert_receive {:ack, ^ref, successful, failed}, 5000
    assert length(successful) == 3
    assert length(failed) == 0
  end

  test "combines the same string twice" do
    ref = Broadway.test_messages(BroadwaySQSExample.String, ["1", "2", "3"])
    assert_receive {:ack, ^ref, successful, failed}, 5000
    assert length(successful) == 3
    assert length(failed) == 0

    data = Enum.reduce(successful, [], fn x, acc -> acc ++ [x.data] end)
    sorted = Enum.sort(data)
    assert sorted == ["11", "22", "33"]
  end
end
