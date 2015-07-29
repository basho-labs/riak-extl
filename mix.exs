defmodule RiakExtl.Mixfile do
  use Mix.Project

  def project do
    [app: :riakextl,
     version: "0.0.2",
     elixir: "~> 1.0",
     escript: [main_module: RiakExtl, name: "riak-extl"],
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps,
     test_coverage: [tool: ExCoveralls]
    ]
  end

  def application do
    [applications: [:logger]]
  end

  defp deps do
    [
      {:riak, github: "glickbot/riak-elixir-client", branch: "master"},
      {:timex, "~> 0.15.0"},
      {:logger_file_backend, "~> 0.0.3"},
      {:json, "~> 0.3.0"},
      {:meck, github: "eproxus/meck", tag: "0.8.2", override: true},
      {:mock, "~> 0.1.1"},
      {:excoveralls, "~> 0.3", only: :test}
    ]
  end
end
