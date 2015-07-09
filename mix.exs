defmodule RiakExtl.Mixfile do
  use Mix.Project

  def project do
    [app: :riakextl,
     version: "0.0.2",
     elixir: "~> 1.0",
     escript: [main_module: RiakExtl, name: "riak-extl"],
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  def application do
    [applications: [:logger]]
  end

  defp deps do
    [
      {:riak, github: "drewkerrigan/riak-elixir-client"},
      {:timex, "~> 0.15.0"},
      {:logger_file_backend, "~> 0.0.3"},
      {:json, "~> 0.3.0"},
      {:meck, github: "eproxus/meck", tag: "0.8.2", override: true},
      {:riak_pb, github: "basho/riak_pb", override: true, tag: "2.0.0.16", compile: "./rebar get-deps compile deps_dir=../"},
      {:riakc, github: "basho/riak-erlang-client", tag: "2.0.1"}
    ]
  end
end
