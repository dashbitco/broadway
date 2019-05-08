defmodule IntQueueTest do
  use ExUnit.Case, async: true
  doctest BroadwaySQSExample.IntSquared

  test "Acks work" do
    ref = Broadway.test_messages(BroadwaySQSExample.IntSquared, ["1", "2", "3"])
    assert_receive {:ack, ^ref, successful, failed}, 5000
    assert length(successful) == 3
    assert length(failed) == 0
  end

  test "squares numbers" do
    ref = Broadway.test_messages(BroadwaySQSExample.IntSquared, ["1", "2", "3"])
    assert_receive {:ack, ^ref, successful, failed}, 5000
    assert length(successful) == 3
    assert length(failed) == 0

    data = Enum.reduce(successful, [], fn x, acc -> acc ++ [x.data] end)
    sorted = Enum.sort(data)
    assert sorted == [1, 4, 9]
  end
end
