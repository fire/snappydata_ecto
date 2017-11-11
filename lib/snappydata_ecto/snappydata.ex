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

  def execute_ddl(repo, definition, opts) do
      definition = definition
      |> check_for_empty_prefix
      |> upcase_table
      execute_sql(repo, definition, opts)
      :ok
  end

  def upcase_table({type, %Table{} = table, columns}) do
    table = %{table | name: String.upcase to_string table.name}
    table = %{table | prefix: String.upcase table.prefix}
    {type, table, columns}
  end

  def upcase_table({type, %Ecto.Migration.Index{} = index}) do
    index = %{index | name: String.upcase to_string index.name}
    index = %{index | prefix: String.upcase index.prefix}
    {type, index}
  end

  def check_for_empty_prefix({type, %Table{} = table, columns}) do
    table = case Map.get(table, :prefix) do
              nil -> %{table | prefix: "APP"}
              _ -> table
            end
    {type, table, columns}
  end

  def check_for_empty_prefix({type, %Ecto.Migration.Index{} = index}) do
    index = case Map.get(index, :prefix) do
              nil -> %{index | prefix: "APP"}
              _ -> index
            end
    {type, index}
  end

  def execute_sql(repo, definition = {:create_if_not_exists, %Table{} = table, columns}, opts) do
    execute_sql_if_exist(repo, definition, table, opts)
  end

  def execute_sql_if_exist(repo, definition, table, opts) do
    sql_if_exist = "SELECT tablename " <>
      "FROM sys.systables " <>
      "WHERE TABLESCHEMANAME = '#{table.prefix}' AND TABLENAME = '#{table.name}'"
    Logger.debug "#{inspect self()} execute_sql_if_exist queried " <> sql_if_exist
    unless extract_table_row(Ecto.Adapters.SQL.query!(repo, sql_if_exist, [], opts)) do
      sql = @conn.execute_ddl(definition)
      Logger.debug "#{inspect self()} unless extract_table_row queried " <> sql
      Ecto.Adapters.SQL.query!(repo, sql, [], opts)
    end
  end

  def execute_sql(repo, definition, opts) do
    sql = @conn.execute_ddl(definition)
    Logger.debug "#{inspect self()} execute_sql queried " <> sql
    Ecto.Adapters.SQL.query!(repo, sql, [], opts)
  end

  def extract_table_row(%Snappyex.Result{rows: [[table]]}) do
    table
  end

  def extract_table_row(%Snappyex.Result{rows: []}) do
    nil
  end

  
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
