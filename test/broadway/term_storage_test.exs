defmodule Broadway.TermStorageTest do
  use ExUnit.Case, async: true

  doctest Broadway.TermStorage
  alias Broadway.TermStorage

  test "allows terms to be written and read" do
    ref = TermStorage.put({:really, :unique, :term})
    assert TermStorage.get!(ref) == {:really, :unique, :term}
  end

  test "returns the same reference for the same term" do
    assert TermStorage.put({:really, :unique, :term}) ==
             TermStorage.put({:really, :unique, :term})
  end

  test "returns different references for different terms" do
    assert TermStorage.put({:really, :unique, :term}) !=
             TermStorage.put({:another, :unique, :term})
  end
end
