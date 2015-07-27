use Mix.Config

config :riakextl,
  src_ip: "127.0.0.1",
  src_port: 10037,
  sink_ip: "127.0.0.1",
  sink_port: 10047,
  src_dir: "./dump/source",
  sink_dir: "./dump/sink"
