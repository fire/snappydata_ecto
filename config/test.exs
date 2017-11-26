use Mix.Config

config :snappydata_ecto, Hello.Repo,
  adapter: Ecto.Adapters.SnappyData,
  username: "app",
  password: "",
  hostname: "192.168.0.17",
  pool: DBConnection.Poolboy,
  pool_size: 20,
  schema: "app",
# loggers: [{IO, :inspect, []}],
  port: 1527

# Do not include metadata nor timestamps in development logs
config :logger, :console, format: "[$level] $message\n"
