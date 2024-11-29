defmodule Broadway.ConfigStorageTest do
  use ExUnit.Case, async: false
  alias Broadway.ConfigStorage.Ets

  setup do
    prev = Application.fetch_env!(:broadway, :config_storage)

    on_exit(fn ->
      Application.put_env(:broadway, :config_storage, prev)
    end)
  end

  test "ets default options" do
    Application.put_env(:broadway, :config_storage, :ets)
    Ets.setup()
    assert [] = Ets.list()
    assert Ets.put("some name", %Broadway.Topology{})
    assert ["some name"] = Ets.list()
    assert %Broadway.Topology{} = Ets.get("some name")
    assert :ets.info(Ets.table(), :size) == 1
    Ets.delete("some name")
    assert :ets.info(Ets.table(), :size) == 0
  end
end
