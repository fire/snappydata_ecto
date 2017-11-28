defmodule Ecto.Adapters.SnappyData do

  # BASED ON https://github.com/elixir-ecto/ecto/blob/master/test/ecto/adapters/postgres_test.exs
  # Try to keep it synced

  @moduledoc """
  """

  # Inherit all behaviour from Ecto.Adapters.SQL
  use Ecto.Adapters.SQL, :snappyex
  require Logger

  # And provide a custom storage implementation
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  @doc false
  def storage_up(opts) do
    schema = Keyword.fetch!(opts, :schema) || raise ":schema is nil in repository configuration"
    opts     = Keyword.put(opts, :schema, "snappydata")

    command =
      ~s(CREATE SCHEMA "#{schema}")

    case run_query(command, opts) do
      {:ok, _} ->
        :ok
      {:error, %{snappydata: %{code: :duplicate_database}}} ->
        {:error, :already_up}
      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  @doc false
  def storage_down(opts) do
    schema = Keyword.fetch!(opts, :schema) || raise ":schema is nil in repository configuration"
    command  = "DROP SCHEMA \"#{schema}\""
    opts     = Keyword.put(opts, :schema, "snappydata")

    case run_query(command, opts) do
      {:ok, _} ->
        :ok
      {:error, %{snappydata: %{code: :invalid_catalog_name}}} ->
        {:error, :already_down}
      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  @doc false
  def supports_ddl_transaction? do
    false
  end
  alias Ecto.Migration.{Table, Index, Reference, Constraint}
  @conn __MODULE__.Connection

  ## Helpers

  defp run_query(sql, opts) do
    {:ok, _} = Application.ensure_all_started(:snappyex)

    opts =
      opts
      |> Keyword.drop([:name, :log])

    {:ok, pid} = Task.Supervisor.start_link

    task = Task.Supervisor.async_nolink(pid, fn ->
      {:ok, conn} = Snappyex.start_link(opts)

      value = Ecto.Adapters.SnappyData.Connection.execute(conn, sql, [], opts)
      GenServer.stop(conn)
      value
    end)

    timeout = Keyword.get(opts, :timeout, 15_000)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, {:ok, result}} ->
        {:ok, result}
      {:ok, {:error, error}} ->
        {:error, error}
      {:exit, {%{__struct__: struct} = error, _}}
          when struct in [SnappyData.Error, DBConnection.Error] ->
        {:error, error}
      {:exit, reason}  ->
        {:error, RuntimeError.exception(Exception.format_exit(reason))}
      nil ->
        {:error, RuntimeError.exception("command timed out")}
    end
  end
end
