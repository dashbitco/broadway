defmodule Broadway.ConfigStorageTest do
  use ExUnit.Case, async: false

  alias Broadway.ConfigStorage.ETS

  setup do
    prev = Application.fetch_env!(:broadway, :config_storage)

    on_exit(fn ->
      Application.put_env(:broadway, :config_storage, prev)
    end)
  end

  test "ets default options" do
    Application.put_env(:broadway, :config_storage, :ets)
    ETS.setup()
    assert [] = ETS.list()

    assert ETS.put("some name", %Broadway.Topology{})
    assert ["some name"] = ETS.list()
    assert %Broadway.Topology{} = ETS.get("some name")
    assert :ets.info(ETS.table(), :size) == 1

    ETS.delete("some name")
    assert :ets.info(ETS.table(), :size) == 0
  end
end
