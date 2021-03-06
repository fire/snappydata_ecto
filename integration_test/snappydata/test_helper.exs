

Logger.configure(level: :debug)

ExUnit.start exclude: [:array_type, :read_after_writes, :returning,
                       :strict_savepoint, :create_index_if_not_exists],
             max_cases: 1

# Configure Ecto for support and tests
Application.put_env(:ecto, :lock_for_update, "FOR UPDATE")
Application.put_env(:ecto, :primary_key_type, :id)

# Configure SnappyData connection
#Application.put_env(:ecto, :snappydata_test_url,
#  "ecto://" <> (System.get_env("SNAPPYDATA_URL") || "localhost:1527")
#)

# Load support files
Code.require_file "../../deps/ecto/integration_test/support/repo.exs", __DIR__
Code.require_file "../../deps/ecto/integration_test/support/schemas.exs", __DIR__
Code.require_file "../../deps/ecto/integration_test/support/migration.exs", __DIR__

pool =
  case System.get_env("ECTO_POOL") || "poolboy" do
    "poolboy"        -> DBConnection.Poolboy
    "sojourn_broker" -> DBConnection.Sojourn
  end

# Pool repo for async, safe tests
alias Ecto.Integration.TestRepo

require Thrift.Generated.SecurityMechanism

Application.put_env(:ecto, TestRepo,
  adapter: Ecto.Adapters.SnappyData,
  hostname: System.get_env("SNAPPYDATA_HOST") || "localhost",
  port: System.get_env("SNAPPYDATA_PORT") || 1527,
  opts: [ssl_opts: [enabled: true, cacertfile: "config/cacerts.pem", certfile: "config/cert.pem", keyfile: "config/key.pem"]],
  pool: Ecto.Adapters.SQL.Sandbox,
  ownership_pool: pool,
  client_host_name: "localhost", 
  client_id: "ElixirClient1|0x" <> Base.encode16(inspect self()),
  properties: %{"load-balance" => "false"}, 
  schema: "app",
  for_xa: false,
  ownership_timeout: 30_000,
  security:  Thrift.Generated.SecurityMechanism.plain, 
  token_size: 16,
  use_string_for_decimal: false,
  username: "APP",
  password: "APP")

defmodule Ecto.Integration.TestRepo do
  use Ecto.Integration.Repo, otp_app: :ecto

  def set_sandbox_mode() do
    Ecto.Adapters.SQL.Sandbox.mode(TestRepo, :manual)
    Process.flag(:trap_exit, true)
  end
end

# Pool repo for non-async tests
alias Ecto.Integration.PoolRepo

Application.put_env(:ecto, PoolRepo,
  adapter: Ecto.Adapters.SnappyData,
  pool: pool,
  pool_size: 10,
  hostname: System.get_env("SNAPPYDATA_HOST") || "localhost",
  port: System.get_env("SNAPPYDATA_PORT") || 1527,
  client_host_name: "localhost", 
  client_id: "ElixirClient1|0x" <> Base.encode16(inspect self()),
  properties: %{"load-balance" => "false"},
  schema: "app", 
  for_xa: false,
  ownership_timeout: 30_000,
  security:  Thrift.Generated.SecurityMechanism.plain, 
  token_size: 16,
  use_string_for_decimal: false,
  username: "APP",
  password: "APP")

defmodule Ecto.Integration.PoolRepo do
  use Ecto.Integration.Repo, otp_app: :ecto

  def create_prefix(prefix) do
    "create schema #{prefix}"
  end

  def drop_prefix(prefix) do
    "drop schema #{prefix}"
  end
end

defmodule Ecto.Integration.Case do
  use ExUnit.CaseTemplate

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(TestRepo)
  end
end

{:ok, _} = Application.ensure_all_started(:snappyex)

{:ok, _pid} = TestRepo.start_link
{:ok, _pid} = PoolRepo.start_link
case Ecto.Migrator.up(TestRepo, 0, Ecto.Integration.Migration, log: false) do
  :ok -> Ecto.Integration.TestRepo.set_sandbox_mode()
  :already_up -> nil
end
