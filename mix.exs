defmodule SnappydataEcto.Mixfile do
  use Mix.Project

  @version "0.1.0"
  @adapters [:snappydata]

  def project do
    [app: :snappydata_ecto,
     version: @version,
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps(),
     test_paths: test_paths(Mix.env),
     aliases: ["test.all": ["test", "test.adapters"],
               "test.adapters": &test_adapters/1],
     preferred_cli_env: ["test.all": :test]]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    # Specify extra applications you'll use from Erlang/Elixir
    [extra_applications: [:logger, :ecto]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:my_dep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:my_dep, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [{:earmark, "~> 0.1", only: :dev},
     {:ex_doc, "~> 0.7", only: :dev},
     {:ecto, "~> 2.2"},
     {:snappyex, git: "https://github.com/fire/snappyex.git", tag: "master"} ]
  end

  defp test_paths(adapter) when adapter in @adapters, do: ["integration_test/#{adapter}"]
  defp test_paths(_), do: ["test/ecto", "test/mix"]

  defp test_adapters(args) do
    for env <- @adapters, do: env_run(env, args)
  end

  defp env_run(env, args) do
    args = if IO.ANSI.enabled?, do: ["--color"|args], else: ["--no-color"|args]

    IO.puts "==> Running tests for MIX_ENV=#{env} mix test"
    {_, res} = System.cmd "mix", ["test"|args],
                          into: IO.binstream(:stdio, :line),
                          env: [{"MIX_ENV", to_string(env)}]

    if res > 0 do
      System.at_exit(fn _ -> exit({:shutdown, 1}) end)
    end
  end
end
