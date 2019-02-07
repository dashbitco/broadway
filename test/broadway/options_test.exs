defmodule Broadway.OptionsTest do
  use ExUnit.Case

  alias Broadway.Options

  test "known options" do
    spec = [name: [], context: []]
    opts = [name: MyProducer, context: :ok]

    assert Options.validate(opts, spec) == {:ok, opts}
  end

  test "unknown options" do
    spec = [an_option: [], other_option: []]
    opts = [an_option: 1, not_an_option1: 1, not_an_option2: 1]

    assert Options.validate(opts, spec) ==
             {:error,
              "unknown options [:not_an_option1, :not_an_option2], valid options are: [:an_option, :other_option]"}
  end

  test "options with default values" do
    spec = [context: [default: :ok]]

    assert Options.validate([], spec) == {:ok, [context: :ok]}
  end

  test "all required options present" do
    spec = [name: [required: true, type: :atom]]
    opts = [name: MyProducer]

    assert Options.validate(opts, spec) == {:ok, opts}
  end

  test "required options missing" do
    spec = [name: [required: true], an_option: [], other_option: []]
    opts = [an_option: 1, other_option: 2]

    assert Options.validate(opts, spec) ==
             {:error,
              "required option :name not found, received options: [:an_option, :other_option]"}
  end

  describe "type validation" do
    test "valid positive integer" do
      spec = [stages: [type: :pos_integer]]
      opts = [stages: 1]

      assert Options.validate(opts, spec) == {:ok, opts}
    end

    test "invalid positive integer" do
      spec = [stages: [type: :pos_integer]]

      assert Options.validate([stages: 0], spec) ==
               {:error, "expected :stages to be a positive integer, got: 0"}

      assert Options.validate([stages: :an_atom], spec) ==
               {:error, "expected :stages to be a positive integer, got: :an_atom"}
    end

    test "valid non negative integer" do
      spec = [min_demand: [type: :non_neg_integer]]
      opts = [min_demand: 0]

      assert Options.validate(opts, spec) == {:ok, opts}
    end

    test "invalid non negative integer" do
      spec = [min_demand: [type: :non_neg_integer]]

      assert Options.validate([min_demand: -1], spec) ==
               {:error, "expected :min_demand to be a non negative integer, got: -1"}

      assert Options.validate([min_demand: :an_atom], spec) ==
               {:error, "expected :min_demand to be a non negative integer, got: :an_atom"}
    end

    test "valid atom" do
      spec = [name: [type: :atom]]
      opts = [name: :an_atom]
      assert Options.validate(opts, spec) == {:ok, opts}
    end

    test "invalid atom" do
      spec = [name: [type: :atom]]

      assert Options.validate([name: 1], spec) == {:error, "expected :name to be an atom, got: 1"}
    end

    test "valid mfa" do
      spec = [transformer: [type: :mfa]]

      opts = [transformer: {SomeMod, :func, [1, 2]}]
      assert Options.validate(opts, spec) == {:ok, opts}

      opts = [transformer: {SomeMod, :func, []}]
      assert Options.validate(opts, spec) == {:ok, opts}
    end

    test "invalid mfa" do
      spec = [transformer: [type: :mfa]]

      opts = [transformer: {"not_a_module", :func, []}]

      assert Options.validate(opts, spec) == {
               :error,
               ~s(expected :transformer to be a tuple {Mod, Fun, Args}, got: {"not_a_module", :func, []})
             }

      opts = [transformer: {SomeMod, "not_a_func", []}]

      assert Options.validate(opts, spec) == {
               :error,
               ~s(expected :transformer to be a tuple {Mod, Fun, Args}, got: {SomeMod, "not_a_func", []})
             }

      opts = [transformer: {SomeMod, :func, "not_a_list"}]

      assert Options.validate(opts, spec) == {
               :error,
               ~s(expected :transformer to be a tuple {Mod, Fun, Args}, got: {SomeMod, :func, "not_a_list"})
             }
    end
  end

  describe "nested options with predefined keys" do
    test "known options" do
      spec = [
        processors: [
          type: :keyword_list,
          keys: [
            stages: [],
            max_demand: []
          ]
        ]
      ]

      opts = [processors: [stages: 1, max_demand: 2]]

      assert Options.validate(opts, spec) == {:ok, opts}
    end

    test "unknown options" do
      spec = [
        processors: [
          type: :keyword_list,
          keys: [
            stages: [],
            min_demand: []
          ]
        ]
      ]

      opts = [
        processors: [
          stages: 1,
          unknown_option1: 1,
          unknown_option2: 1
        ]
      ]

      assert Options.validate(opts, spec) ==
               {:error,
                "unknown options [:unknown_option1, :unknown_option2], valid options are: [:stages, :min_demand]"}
    end

    test "options with default values" do
      spec = [
        processors: [
          type: :keyword_list,
          keys: [
            stages: [default: 10]
          ]
        ]
      ]

      opts = [processors: []]

      assert Options.validate(opts, spec) == {:ok, [processors: [stages: 10]]}
    end

    test "all required options present" do
      spec = [
        processors: [
          type: :keyword_list,
          keys: [
            stages: [required: true],
            max_demand: [required: true]
          ]
        ]
      ]

      opts = [processors: [stages: 1, max_demand: 2]]

      assert Options.validate(opts, spec) == {:ok, opts}
    end

    test "required options missing" do
      spec = [
        processors: [
          type: :keyword_list,
          keys: [
            stages: [required: true],
            max_demand: [required: true]
          ]
        ]
      ]

      opts = [processors: [max_demand: 1]]

      assert Options.validate(opts, spec) ==
               {:error, "required option :stages not found, received options: [:max_demand]"}
    end

    test "nested options types" do
      spec = [
        processors: [
          type: :keyword_list,
          keys: [
            name: [type: :atom],
            stages: [type: :pos_integer]
          ]
        ]
      ]

      opts = [processors: [name: MyModule, stages: :an_atom]]

      assert Options.validate(opts, spec) ==
               {:error, "expected :stages to be a positive integer, got: :an_atom"}
    end
  end

  describe "nested options with custom keys" do
    test "known options" do
      spec = [
        producers: [
          type: :keyword_list,
          keys: [
            *: [
              module: [],
              arg: []
            ]
          ]
        ]
      ]

      opts = [producers: [producer1: [module: MyModule, arg: :ok]]]

      assert Options.validate(opts, spec) == {:ok, opts}
    end

    test "unknown options" do
      spec = [
        producers: [
          type: :keyword_list,
          keys: [
            *: [
              module: [],
              arg: []
            ]
          ]
        ]
      ]

      opts = [producers: [producer1: [module: MyModule, arg: :ok, unknown_option: 1]]]

      assert Options.validate(opts, spec) ==
               {:error, "unknown options [:unknown_option], valid options are: [:module, :arg]"}
    end

    test "options with default values" do
      spec = [
        producers: [
          type: :keyword_list,
          keys: [
            *: [
              arg: [default: :ok]
            ]
          ]
        ]
      ]

      opts = [producers: [producer1: []]]

      assert Options.validate(opts, spec) == {:ok, [producers: [producer1: [arg: :ok]]]}
    end

    test "all required options present" do
      spec = [
        producers: [
          type: :keyword_list,
          keys: [
            *: [
              module: [required: true],
              arg: [required: true]
            ]
          ]
        ]
      ]

      opts = [producers: [default: [module: MyModule, arg: :ok]]]

      assert Options.validate(opts, spec) == {:ok, opts}
    end

    test "required options missing" do
      spec = [
        producers: [
          type: :keyword_list,
          keys: [
            *: [
              module: [required: true],
              arg: [required: true]
            ]
          ]
        ]
      ]

      opts = [producers: [default: [module: MyModule]]]

      assert Options.validate(opts, spec) ==
               {:error, "required option :arg not found, received options: [:module]"}
    end

    test "nested options types" do
      spec = [
        producers: [
          type: :keyword_list,
          keys: [
            *: [
              module: [required: true, type: :atom],
              stages: [type: :pos_integer]
            ]
          ]
        ]
      ]

      opts = [
        producers: [
          producer1: [
            module: MyProducer,
            stages: :an_atom
          ]
        ]
      ]

      assert Options.validate(opts, spec) ==
               {:error, "expected :stages to be a positive integer, got: :an_atom"}
    end

    test "validate empty keys" do
      spec = [
        producers: [
          type: :keyword_list,
          keys: [
            *: [
              module: [required: true, type: :atom],
              stages: [type: :pos_integer]
            ]
          ]
        ]
      ]

      opts = [
        producers: []
      ]

      assert Options.validate(opts, spec) ==
               {:error, "expected :producers to be a non-empty keyword list, got: []"}
    end
  end
end
