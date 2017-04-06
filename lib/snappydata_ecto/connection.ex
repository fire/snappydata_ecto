if Code.ensure_loaded?(Snappyex) do

  defmodule Ecto.Adapters.SnappyData.Connection do
    @moduledoc false

    @default_port 1527
    @default_host "127.0.0.1"
    @behaviour Ecto.Adapters.SQL.Connection

    def child_spec(opts) do
      opts =
        opts
        |> Keyword.update(:port, @default_port, &normalize_port/1)
        |> Keyword.put(:types, true)
      Snappyex.child_spec(opts)
    end

    def init({repo, opts}) do
      opts
    end

    defp normalize_port(port) when is_binary(port), do: String.to_integer(port)
    defp normalize_port(port) when is_integer(port), do: port

    ## Query

    def prepare_execute(conn, name, sql, params, opts) do
      query = %Snappyex.Query{name: name, statement: sql}
      DBConnection.prepare_execute(conn, query, params, opts)
    end

    def execute(conn, sql, params, opts) when is_binary(sql) do
      query = %Snappyex.Query{name: "", statement: sql}
      case DBConnection.prepare_execute(conn, query, params, opts) do
        {:ok, _, query} -> {:ok, query}
        {:error, err} -> {:error, err}
      end
    end

    def execute(conn, %{} = query, params, opts) do
      DBConnection.execute(conn, query, params, opts)
    end

    ## Query

    alias Ecto.Query
    alias Ecto.Query.{BooleanExpr, JoinExpr, QueryExpr}

    def insert(prefix, table, header, rows, _on_conflict, returning) do
      prefix = unless prefix do
        "APP"
      end

      values =
        if header == [] do
          "VALUES " <> Enum.map_join(rows, ",", fn _ -> "(DEFAULT)" end)
        else
          "(" <> Enum.map_join(header, ",", &quote_name/1) <> ") " <>
          "VALUES " <> insert_all(rows, 1, "")
        end

      assemble(["INSERT INTO #{quote_table(prefix, table)}", "",
                values])
    end

    defp on_conflict({:raise, _, []}, _header) do
      error!(nil, "on_conflict is not supported by SnappyData")
    end
    defp on_conflict({:nothing, _, []}, [field | _]) do
      error!(nil, "on_conflict is not supported by SnappyData")
    end
    defp on_conflict({query, _, []}, _header) do
      error!(nil, "on_conflict is not supported by SnappyData")
    end

    defp insert_each([nil|t], counter, acc),
      do: insert_each(t, counter, acc <> ",DEFAULT")
    defp insert_each([_|t], counter, acc),
      do: insert_each(t, counter + 1, acc <> ",$" <> Integer.to_string(counter))
    defp insert_each([], counter, "," <> acc),
      do: {counter, acc}

    defp insert_all([row|rows], counter, acc) do
      {counter, row} = insert_each(row, counter, "")
      insert_all(rows, counter, acc <> ",(" <> row <> ")")
    end
    defp insert_all([], _counter, "," <> acc) do
      acc
    end

    defp insert_as({%{from: from} = query, _, _}) do
      {_, name} = get_source(%{query | joins: []}, create_names(query), 0, from)
      error!(nil, "insert_as is not supported by SnappyData")
    end
    defp insert_as({_, _, _}) do
      []
    end

    def all(query) do
      sources        = create_names(query)
      distinct_exprs = distinct_exprs(query, sources)

      from     = from(query, sources)
      select   = select(query, distinct_exprs, sources)
      join     = join(query, sources)
      where    = where(query, sources)
      group_by = group_by(query, sources)
      having   = having(query, sources)
      order_by = order_by(query, distinct_exprs, sources)
      limit    = limit(query, sources)
      offset   = offset(query, sources)
      lock     = lock(query.lock)

      assemble([select, from, join, where, group_by, having, order_by, limit, offset, lock])
    end

    defp distinct_exprs(_, _), do: ""

    defp from(%{from: from} = query, sources) do
      {from, name} = get_source(query, sources, 0, from)
      "FROM #{from} AS #{name}"
    end

    defp select(%Query{select: %{fields: fields}, distinct: distinct} = query,
                distinct_exprs, sources) do
      "SELECT " <>
        distinct(distinct, distinct_exprs) <>
        select_fields(fields, sources, query)
    end

    defp join(%Query{joins: []}, _sources), do: []
    defp join(%Query{joins: joins} = query, sources) do
      Enum.map_join(joins, " ", fn
        %JoinExpr{on: %QueryExpr{expr: expr}, qual: qual, ix: ix, source: source} ->
          {join, name} = get_source(query, sources, ix, source)
          qual = join_qual(qual)
          qual <> " " <> join <> " AS " <> name <> " ON " <> expr(expr, sources, query)
      end)
    end

    defp where(%Query{wheres: wheres} = query, sources) do
      boolean("WHERE", wheres, sources, query)
    end

    defp group_by(%Query{group_bys: group_bys} = query, sources) do
      exprs =
        Enum.map_join(group_bys, ", ", fn
          %QueryExpr{expr: expr} ->
            Enum.map_join(expr, ", ", &expr(&1, sources, query))
        end)

      case exprs do
        "" -> []
        _  -> "GROUP BY " <> exprs
      end
    end

    defp order_by(%Query{order_bys: order_bys} = query, distinct_exprs, sources) do
      exprs =
        Enum.map_join(order_bys, ", ", fn
          %QueryExpr{expr: expr} ->
            Enum.map_join(expr, ", ", &order_by_expr(&1, sources, query))
        end)

      case {distinct_exprs, exprs} do
        {_, ""} ->
          []
        {"", _} ->
          "ORDER BY " <> exprs
        {_, _}  ->
          "ORDER BY " <> distinct_exprs <> ", " <> exprs
      end
    end

    defp having(%Query{havings: havings} = query, sources) do
      boolean("HAVING", havings, sources, query)
    end

   defp limit(%Query{limit: nil}, _sources), do: []
    defp limit(%Query{limit: %QueryExpr{expr: expr}} = query, sources) do
      "LIMIT " <> expr(expr, sources, query)
    end

    defp offset(%Query{offset: nil}, _sources), do: []
    defp offset(%Query{offset: %QueryExpr{expr: expr}} = query, sources) do
      "OFFSET " <> expr(expr, sources, query)
    end

    defp lock(nil), do: []
    defp lock(lock_clause), do: lock_clause

    defp boolean(_name, [], _sources, _query), do: []
    defp boolean(name, [%{expr: expr, op: op} | query_exprs], sources, query) do
      [name |
       Enum.reduce(query_exprs, {op, paren_expr(expr, sources, query)}, fn
         %BooleanExpr{expr: expr, op: op}, {op, acc} ->
           {op, [acc, operator_to_boolean(op), paren_expr(expr, sources, query)]}
         %BooleanExpr{expr: expr, op: op}, {_, acc} ->
           {op, [?(, acc, ?), operator_to_boolean(op), paren_expr(expr, sources, query)]}
       end) |> elem(1)]
    end

    defp operator_to_boolean(:and), do: " AND "
    defp operator_to_boolean(:or), do: " OR "

    defp paren_expr(expr, sources, query) do
      [?(, expr(expr, sources, query), ?)]
    end

    defp assemble(list) do
      list
      |> List.flatten
      |> Enum.join(" ")
    end

    defp create_names(%{prefix: prefix, sources: sources}) do
      create_names(prefix, sources, 0, tuple_size(sources)) |> List.to_tuple()
    end

    defp create_names(prefix, sources, pos, limit) when pos < limit do
      current =
        case elem(sources, pos) do
          {table, schema} ->
            name = String.first(table) <> Integer.to_string(pos)
            {quote_table(prefix, table), name, schema}
          {:fragment, _, _} ->
            {nil, "f" <> Integer.to_string(pos), nil}
          %Ecto.SubQuery{} ->
            {nil, "s" <> Integer.to_string(pos), nil}
        end
      [current|create_names(prefix, sources, pos + 1, limit)]
    end

    defp create_names(_prefix, _sources, pos, pos) do
      []
    end

    defp distinct(nil, _sources), do: ""
    defp distinct(%QueryExpr{expr: true}, _exprs),  do: "DISTINCT "
    defp distinct(%QueryExpr{expr: false}, _exprs), do: ""
    defp distinct(_query, exprs), do: "DISTINCT ON (" <> exprs <> ") "

    defp select_fields([], _sources, _query),
      do: "TRUE"
    defp select_fields(fields, sources, query),
      do: Enum.map_join(fields, ", ", &expr(&1, sources, query))

    defp join_qual(:inner), do: "INNER JOIN"
    defp join_qual(:inner_lateral), do: "INNER JOIN LATERAL"
    defp join_qual(:left),  do: "LEFT OUTER JOIN"
    defp join_qual(:left_lateral),  do: "LEFT OUTER JOIN LATERAL"
    defp join_qual(:right), do: "RIGHT OUTER JOIN"
    defp join_qual(:full),  do: "FULL OUTER JOIN"

    defp index_expr(literal) when is_binary(literal),
      do: literal
    defp index_expr(literal),
      do: quote_name(literal)

    defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _query) when is_atom(field) do
      {_, name, _} = elem(sources, idx)
      "#{name}.#{field}"
    end

    defp expr({:&, _, [idx, fields, _counter]}, sources, query) do
      {_, name, schema} = elem(sources, idx)
      if is_nil(schema) and is_nil(fields) do
        error!(query, "SnappyData requires a schema module when using selector " <>
          "#{inspect name} but none was given. " <>
          "Please specify a schema or specify exactly which fields from " <>
          "#{inspect name} you desire")
      end
      Enum.map_join(fields, ", ", &"#{name}.#{quote_name(&1)}")
    end

    defp expr({:in, _, [_left, []]}, _sources, _query) do
      "false"
    end

    defp expr({:in, _, [left, right]}, sources, query) when is_list(right) do
      args = Enum.map_join right, ",", &expr(&1, sources, query)
      expr(left, sources, query) <> " IN (" <> args <> ")"
    end

    defp expr({:in, _, [left, {:^, _, [ix, _]}]}, sources, query) do
      expr(left, sources, query) <> " = ANY($#{ix+1})"
    end

    defp expr({:in, _, [left, right]}, sources, query) do
      expr(left, sources, query) <> " = ANY(" <> expr(right, sources, query) <> ")"
    end

    defp expr({:is_nil, _, [arg]}, sources, query) do
      "#{expr(arg, sources, query)} IS NULL"
    end

    defp expr({:not, _, [expr]}, sources, query) do
      "NOT (" <> expr(expr, sources, query) <> ")"
    end

    defp expr(%Ecto.SubQuery{query: query}, _sources, _query) do
      all(query)
    end

    defp expr({:fragment, _, parts}, sources, query) do
      Enum.map_join(parts, "", fn
        {:raw, part}  -> part
        {:expr, expr} -> expr(expr, sources, query)
      end)
    end

    defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

    defp expr(list, sources, query) when is_list(list) do
      "ARRAY[" <> Enum.map_join(list, ",", &expr(&1, sources, query)) <> "]"
    end

    defp expr(%Decimal{} = decimal, _sources, _query) do
      Decimal.to_string(decimal, :normal)
    end

    defp expr(%Ecto.Query.Tagged{value: binary, type: :binary}, _sources, _query)
        when is_binary(binary) do
      hex = Base.encode16(binary, case: :lower)
      "'\\x#{hex}'::bytea"
    end

    defp expr(nil, _sources, _query),   do: "NULL"
    defp expr(true, _sources, _query),  do: "TRUE"
    defp expr(false, _sources, _query), do: "FALSE"

    defp expr(literal, _sources, _query) when is_binary(literal) do
      "'#{escape_string(literal)}'"
    end

    defp expr(literal, _sources, _query) when is_integer(literal) do
      String.Chars.Integer.to_string(literal)
    end

    defp expr(literal, _sources, _query) when is_float(literal) do
      String.Chars.Float.to_string(literal) <> "::float"
    end

    defp order_by_expr({dir, expr}, sources, query) do
      str = expr(expr, sources, query)
      case dir do
        :asc  -> str
        :desc -> str <> " DESC"
      end
    end

    defp quote_name(name)
    defp quote_name(name) when is_atom(name),
      do: quote_name(Atom.to_string(name))
    defp quote_name(name) do
      if String.contains?(name, "\"") do
        error!(nil, "bad field name #{inspect name}")
      end
      #<<?", name::binary, ?">>
      name
      |> String.upcase
    end

    defp quote_table(nil, name) do
      name
      |> quote_table
      |> String.upcase
    end
    defp quote_table(prefix, name) do
      table = quote_table(prefix) <> "." <> quote_table(name)
      String.upcase(table)
    end

    defp quote_table(name) when is_atom(name),
      do: quote_table(Atom.to_string(name))
    defp quote_table(name) do
      if String.contains?(name, "\"") do
        error!(nil, "bad table name #{inspect name}")
      end
      name
    end

    defp options_expr(nil),
      do: ""
    defp options_expr(keyword) when is_list(keyword),
      do: error!(nil, "SnappyData adapter does not support keyword lists in :options")
    defp options_expr(options),
      do: " #{options}"

    ## DDL
    alias Ecto.Migration.{Table, Index, Reference, Constraint}

    @drops [:drop, :drop_if_exists]


    def execute_ddl({command, %Table{}=table, columns})
    when command in [:create, :create_if_not_exists] do
      options       = options_expr(table.options)
      pk_definition = case pk_definition(columns) do
                        nil -> ""
                        pk -> ", #{pk}"
                      end

      "CREATE TABLE" <>
        " #{quote_table(table.prefix, table.name)}" <>
        " (#{column_definitions(table, columns)}#{pk_definition})" <> options
    end

    def execute_ddl({:create_if_not_exists, %Index{}=index}) do
      assemble(["",
                "",
                execute_ddl({:create, index}) <> ";",
                ""])
    end

    def execute_ddl({:create, %Index{}=index}) do
      fields = Enum.map_join(index.columns, ", ", &index_expr/1)

      assemble(["CREATE",
                if_do(index.unique, "UNIQUE"),
                "INDEX",
                quote_name(index.name),
                "ON",
                quote_table(index.prefix, index.table),
                "(#{fields})"])
    end

    def execute_ddl({:create, %Constraint{}=constraint}) do
      "ALTER TABLE #{quote_table(constraint.prefix, constraint.table)} ADD #{new_constraint_expr(constraint)}"
    end

    defp new_constraint_expr(%Constraint{check: check} = constraint) when is_binary(check) do
      "CONSTRAINT #{quote_name(constraint.name)} CHECK (#{check})"
    end
    defp new_constraint_expr(%Constraint{exclude: exclude} = constraint) when is_binary(exclude) do
      "CONSTRAINT #{quote_name(constraint.name)} EXCLUDE USING #{exclude}"
    end

    defp pk_definition(columns) do
      pks =
        for {_, name, _, opts} <- columns,
            opts[:primary_key],
            do: name

      case pks do
        [] -> nil
        _  -> "PRIMARY KEY (" <> Enum.map_join(pks, ", ", &quote_name/1) <> ")"
      end
    end

    defp column_definitions(table, columns) do
      Enum.map_join(columns, ", ", &column_definition(table, &1))
    end

    defp column_definition(table, {:add, name, %Reference{} = ref, opts}) do
      assemble([
        quote_name(name), reference_column_type(ref.type, opts),
        column_options(ref.type, opts), reference_expr(ref, table, name)
      ])
    end

    defp column_definition(_table, {:add, name, type, opts}) do
      assemble([quote_name(name), column_type(type, opts), column_options(type, opts)])
    end

    defp reference_column_type(:serial, _opts), do: "BIGINT"
    defp reference_column_type(type, opts), do: column_type(type, opts)

    defp column_options(type, opts) do
      default = Keyword.fetch(opts, :default)
      null    = Keyword.get(opts, :null)
      [default_expr(default, type), null_expr(null)]
    end

    defp reference_name(%Reference{name: nil}, table, column),
      do: quote_name("#{table.name}_#{column}_fkey")
    defp reference_name(%Reference{name: name}, _table, _column),
      do: quote_name(name)

    # Foreign key definition
    defp reference_expr(%Reference{} = ref, table, name),
      do: "CONSTRAINT #{reference_name(ref, table, name)} REFERENCES " <>
          "#{quote_table(table.prefix, ref.table)}(#{quote_name(ref.column)})" <>
          reference_on_delete(ref.on_delete) <> reference_on_update(ref.on_update)

    defp column_type({:array, type}, opts),
      do: column_type(type, opts) <> "[]"
    defp column_type(type, opts) do
      size      = Keyword.get(opts, :size)
      precision = Keyword.get(opts, :precision)
      scale     = Keyword.get(opts, :scale)
      type_name = ecto_to_db(type)

      cond do
        size            -> "#{type_name}(#{size})"
        precision       -> "#{type_name}(#{precision},#{scale || 0})"
        type == :string -> "#{type_name}(255)"
        true            -> "#{type_name}"
      end
    end

    defp reference_on_delete(:restrict_all), do: " ON DELETE RESTRICT"
    defp reference_on_delete(_), do: ""

    defp reference_on_update(_), do: ""

    defp default_expr({:ok, nil}, _type),
      do: "DEFAULT NULL"
    defp default_expr({:ok, literal}, _type) when is_binary(literal),
      do: "DEFAULT '#{escape_string(literal)}'"
    defp default_expr({:ok, literal}, _type) when is_number(literal) or is_boolean(literal),
      do: "DEFAULT #{literal}"
    defp default_expr({:ok, {:fragment, expr}}, _type),
      do: "DEFAULT #{expr}"
    defp default_expr({:ok, expr}, type),
      do: raise(ArgumentError, "unknown default `#{inspect expr}` for type `#{inspect type}`. " <>
                               ":default may be a string, number, boolean, empty list or a fragment(...)")
    defp default_expr(:error, _),
      do: []

    defp null_expr(false), do: "NOT NULL"
    defp null_expr(true), do: "NULL"
    defp null_expr(_), do: []

    ## Helpers

    defp if_do(condition, value) do
      if condition, do: value, else: []
    end

    defp get_source(query, sources, ix, source) do
      {expr, name, _schema} = elem(sources, ix)
      {expr || "(" <> expr(source, sources, query) <> ")", name}
    end

    defp escape_string(value) when is_binary(value) do
      :binary.replace(value, "'", "''", [:global])
    end

    defp error!(nil, message) do
      raise ArgumentError, message
    end
    defp error!(query, message) do
      raise Ecto.QueryError, query: query, message: message
    end

    defp ecto_to_db(:id),         do: "BIGINT"
    defp ecto_to_db(:binary_id),  do: "VARCHAR(36)"
    defp ecto_to_db(:string),     do: "VARCHAR"
    defp ecto_to_db(:naive_datetime),   do: "TIMESTAMP"
    defp ecto_to_db(:boolean),    do: "SMALLINT"
    defp ecto_to_db(:binary),     do: "BLOB"
    defp ecto_to_db(:text),       do: "STRING"
    defp ecto_to_db(:uuid),       do: "VARCHAR(36)"
    defp ecto_to_db(:map),        do: "STRING"
    defp ecto_to_db({:map, _}),   do: "STRING"
    defp ecto_to_db(:serial),     do: "INTEGER"
    defp ecto_to_db(:bigserial),  do: "BIGINT"
    defp ecto_to_db(other),       do: Atom.to_string(other)

  end
end
