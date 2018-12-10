defmodule Broadway.OptionsTest do
  use ExUnit.Case

  alias Broadway.Options

  describe "validate" do
    test "valid options " do
      spec = [name: [required: true, type: :atom]]
      opts = [name: MyProducer]

      assert Options.validate(opts, spec) == [name: MyProducer]
    end

    test "valid options with default values" do
      spec = [
        processors: [
          required: true,
          type: :keyword_list,
          keys: [
            stages: [type: :pos_integer, default: 10],
            min_demand: [type: :pos_integer, default: 20],
            max_demand: [type: :pos_integer, default: 40]
          ]
        ]
      ]

      opts = [processors: []]

      assert Options.validate(opts, spec) == [
               processors: [
                 max_demand: 40,
                 min_demand: 20,
                 stages: 10
               ]
             ]
    end

    test "unknown option" do
      spec = [an_option: [], other_option: []]
      opts = [an_option: 1, not_an_option: 1]

      assert Options.validate(opts, spec) ==
               {:error,
                "unknown options [:not_an_option], valid options are: [:an_option, :other_option]"}
    end

    test "unknown options of nested options" do
      spec = [
        processors: [
          type: :keyword_list,
          keys: [
            stages: [type: :pos_integer],
            min_demand: [type: :pos_integer, default: 10]
          ]
        ]
      ]

      opts = [
        processors: [
          stages: 1,
          max_demand: 1
        ]
      ]

      assert Options.validate(opts, spec) ==
               {:error,
                "unknown options [:max_demand], valid options are: [:stages, :min_demand]"}
    end

    test "required option" do
      spec = [name: [required: true], an_option: [], other_option: []]
      opts = [an_option: 1, other_option: 2]

      assert Options.validate(opts, spec) ==
               {:error,
                "required option :name not found, received options: [:an_option, :other_option]"}
    end

    test "valid positive integer" do
      spec = [stages: [type: :pos_integer]]
      opts = [stages: 1]

      assert Options.validate(opts, spec) == [stages: 1]
    end

    test "invalid positive integer" do
      spec = [stages: [type: :pos_integer]]

      assert Options.validate([stages: 0], spec) ==
               {:error, "expected :stages to be a positive integer, got: 0"}

      assert Options.validate([stages: :an_atom], spec) ==
               {:error, "expected :stages to be a positive integer, got: :an_atom"}
    end

    test "valid non negative integer" do
      spec = [stages: [type: :non_neg_integer]]

      assert Options.validate([stages: :an_atom], spec) ==
               {:error, "expected :stages to be a non negative integer, got: :an_atom"}
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

      assert Options.validate([name: :an_atom], spec) == [name: :an_atom]
    end

    test "invalid atom" do
      spec = [name: [type: :atom]]

      assert Options.validate([name: 1], spec) == {:error, "expected :name to be a atom, got: 1"}
    end

    test "nested options with predefined keys" do
      spec = [
        processors: [
          type: :keyword_list,
          keys: [
            stages: [type: :pos_integer],
            min_demand: [type: :pos_integer]
          ]
        ]
      ]

      opts = [
        processors: [
          stages: 1,
          min_demand: :an_atom
        ]
      ]

      assert Options.validate(opts, spec) ==
               {:error, "expected :min_demand to be a positive integer, got: :an_atom"}
    end

    test "nested options with custom keys" do
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

    test "validate empty custom keys" do
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
