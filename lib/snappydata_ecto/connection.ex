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

    alias Ecto.Query
    alias Ecto.Query.{BooleanExpr, JoinExpr, QueryExpr}

    binary_ops = [
      ==: " = ",
      !=: " != ",
      <=: " <= ",
      >=: " >= ",
      <: " < ",
      >: " > ",
      and: " AND ",
      or: " OR ",
      ilike: " ILIKE ",
      like: " LIKE "
    ]

    @binary_ops Keyword.keys(binary_ops)

    Enum.map(binary_ops, fn {op, str} ->
      defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
    end)

    defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

    def prepare_execute(conn, name, sql, params, opts) do
      query = %Snappyex.Query{name: name, statement: sql}
      DBConnection.prepare_execute(conn, query, params, opts)
    end

    def execute(conn, sql, params, opts) when is_binary(sql) or is_list(sql) do
      query = %Snappyex.Query{name: "", statement: sql}

      case DBConnection.prepare_execute(conn, query, params, opts) do
        {:ok, _, result} ->
          {:ok, result}

        {:error, err} ->
          {:error, err}
      end
    end

    def execute(conn, %{} = query, params, opts) do
      case DBConnection.execute(conn, query, params, opts) do
        {:ok, _result} = ok ->
          ok

        {:error, err} ->
          {:error, err}
      end
    end

    def insert(prefix, table, header, rows, on_conflict, returning) do
      prefix =
        if prefix == nil do
          "APP"
        else
          prefix
        end

      values =
        if header == [] do
          " VALUES " <> Enum.map_join(rows, ",", fn _ -> "(DEFAULT)" end)
        else
          "(" <>
            Enum.map_join(header, ",", &quote_name/1) <>
            ") " <> "VALUES " <> insert_all(rows, 1, "")
        end

      [
        "INSERT INTO ",
        quote_table(prefix, table),
        insert_as(on_conflict),
        values,
        returning(returning)
      ]
    end

    defp on_conflict({:raise, _, []}, _header), do: error!(nil, "on_conflict is not supported by SnappyData")

    defp on_conflict({:nothing, _, []}, [field | _]) do
      error!(nil, "on_conflict is not supported by SnappyData")
    end

    defp on_conflict({query, _, []}, _header) do
      error!(nil, "on_conflict is not supported by SnappyData")
    end

    defp insert_each([nil | t], counter, acc), do: insert_each(t, counter, acc <> ",DEFAULT")
    defp insert_each([_ | t], counter, acc), do: insert_each(t, counter + 1, acc <> ",?")
    defp insert_each([], counter, "," <> acc), do: {counter, acc}

    defp insert_all([row | rows], counter, acc) do
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

    def update(prefix, table, fields, filters, returning) do
      {fields, count} =
        intersperse_reduce(fields, ", ", 1, fn field, acc ->
          {[quote_name(field), " = ?"], acc + 1}
        end)

      {filters, _count} =
        intersperse_reduce(filters, " AND ", count, fn field, acc ->
          {[quote_name(field), " = ?"], acc + 1}
        end)

      [
        "UPDATE ",
        quote_table(prefix, table),
        " SET ",
        fields,
        " WHERE ",
        filters | returning(returning)
      ]
    end

    def delete(prefix, table, filters, returning) do
      {filters, _} =
        intersperse_reduce(filters, " AND ", 1, fn field, acc ->
          {[quote_name(field), " = ?"], acc + 1}
        end)

      ["DELETE FROM ", quote_table(prefix, table), " WHERE ", filters | returning(returning)]
    end

    def all(query) do
      sources = create_names(query)
      {select_distinct, order_by_distinct} = distinct(query.distinct, sources, query)

      from = from(query, sources)
      select = select(query, select_distinct, sources)
      join = join(query, sources)
      where = where(query, sources)
      group_by = group_by(query, sources)
      having = having(query, sources)
      order_by = order_by(query, order_by_distinct, sources)
      limit = limit(query, sources)
      offset = offset(query, sources)
      lock = lock(query.lock)

      [
        select,
        from,
        join,
        where,
        group_by,
        having,
        order_by,
        limit,
        offset,
        lock
      ]
    end

    # Remove
    defp distinct_exprs(_, _), do: ""

    defp from(%{from: from} = query, sources) do
      {from, name} = get_source(query, sources, 0, from)
      [" FROM ", from, " AS " | [name]]
    end

    defp select(%Query{select: %{fields: fields}} = query, select_distinct, sources) do
      ["SELECT", select_distinct, ?\s | select_fields(fields, sources, query)]
    end

    defp select_fields([], _sources, _query), do: "TRUE"

    defp select_fields(fields, sources, query) do
      intersperse_map(fields, ", ", fn
        {key, value} ->
          [expr(value, sources, query), " AS " | quote_name(key)]

        value ->
          expr(value, sources, query)
      end)
    end

    defp join(%Query{joins: []}, _sources), do: []

    defp join(%Query{joins: joins} = query, sources) do
      Enum.map_join(joins, " ", fn %JoinExpr{
                                     on: %QueryExpr{expr: expr},
                                     qual: qual,
                                     ix: ix,
                                     source: source
                                   } ->
        {join, name} = get_source(query, sources, ix, source)
        [" ", join_qual(qual), " ", join, " AS ", name, " ON " | expr(expr, sources, query)]
      end)
    end

    defp where(%Query{wheres: wheres} = query, sources) do
      boolean(" WHERE ", wheres, sources, query)
    end

    defp group_by(%Query{group_bys: group_bys} = query, sources) do
      exprs =
        Enum.map_join(group_bys, ", ", fn %QueryExpr{expr: expr} ->
          Enum.map_join(expr, ", ", &expr(&1, sources, query))
        end)

      case exprs do
        "" -> []
        _ -> " GROUP BY " <> exprs
      end
    end

    defp order_by(%Query{order_bys: []}, _distinct, _sources), do: []

    defp order_by(%Query{order_bys: order_bys} = query, distinct, sources) do
      order_bys = Enum.flat_map(order_bys, & &1.expr)

      [
        " ORDER BY "
        | intersperse_map(distinct ++ order_bys, ", ", &order_by_expr(&1, sources, query))
      ]
    end

    defp having(%Query{havings: havings} = query, sources) do
      boolean(" HAVING ", havings, sources, query)
    end

    defp limit(%Query{limit: nil}, _sources), do: []

    defp limit(%Query{limit: %QueryExpr{expr: expr}} = query, sources) do
      [" LIMIT " | expr(expr, sources, query)]
    end

    defp offset(%Query{offset: nil}, _sources), do: []

    defp offset(%Query{offset: %QueryExpr{expr: expr}} = query, sources) do
      [" OFFSET " | expr(expr, sources, query)]
    end

    defp lock(nil), do: []
    defp lock(lock_clause), do: [?\s | lock_clause]

    defp boolean(_name, [], _sources, _query), do: []

    defp boolean(name, [%{expr: expr, op: op} | query_exprs], sources, query) do
      [
        name
        | Enum.reduce(query_exprs, {op, paren_expr(expr, sources, query)}, fn
            %BooleanExpr{expr: expr, op: op}, {op, acc} ->
              {op, [acc, operator_to_boolean(op), paren_expr(expr, sources, query)]}

            %BooleanExpr{expr: expr, op: op}, {_, acc} ->
              {op, [?(, acc, ?), operator_to_boolean(op), paren_expr(expr, sources, query)]}
          end)
          |> elem(1)
      ]
    end

    defp operator_to_boolean(:and), do: " AND "
    defp operator_to_boolean(:or), do: " OR "

    defp paren_expr(expr, sources, query) do
      [?(, expr(expr, sources, query), ?)]
    end

    defp expr({:^, [], [ix]}, _sources, _query) do
      [??]
    end

    defp returning(%Query{select: nil}, _sources), do: []

    defp returning(%Query{select: %{fields: fields}} = query, sources),
      do: [" RETURNING " | select_fields(fields, sources, query)]

    defp returning([]), do: []

    defp returning(returning),
      do: [" RETURNING " | intersperse_map(returning, ", ", &quote_name/1)]

    defp create_names(%{prefix: prefix, sources: sources}) do
      create_names(prefix, sources, 0, tuple_size(sources)) |> List.to_tuple()
    end

    defp create_names(prefix, sources, pos, limit) when pos < limit do
      current =
        case elem(sources, pos) do
          {table, schema} ->
            name = create_alias(table) <> Integer.to_string(pos)
            {quote_table(prefix, table), name, schema}

          {:fragment, _, _} ->
            {nil, "f" <> Integer.to_string(pos), nil}

          %Ecto.SubQuery{} ->
            {nil, "s" <> Integer.to_string(pos), nil}
        end

      [current | create_names(prefix, sources, pos + 1, limit)]
    end

    defp create_names(_prefix, _sources, pos, pos) do
      []
    end

    defp create_alias(<<first, _rest::binary>>) when first in ?a..?z when first in ?A..?Z do
      <<first>>
    end

    defp create_alias(_) do
      "t"
    end

    defp distinct(nil, _, _), do: {[], []}
    defp distinct(%QueryExpr{expr: []}, _, _), do: {[], []}
    defp distinct(%QueryExpr{expr: true}, _, _), do: {" DISTINCT", []}
    defp distinct(%QueryExpr{expr: false}, _, _), do: {[], []}

    defp distinct(%QueryExpr{expr: exprs}, sources, query) do
      {[
         " DISTINCT ON (",
         intersperse_map(exprs, ", ", fn {_, expr} -> expr(expr, sources, query) end),
         ?)
       ], exprs}
    end

    defp join_qual(:inner), do: "INNER JOIN"
    defp join_qual(:inner_lateral), do: "INNER JOIN LATERAL"
    defp join_qual(:left), do: "LEFT OUTER JOIN"
    defp join_qual(:left_lateral), do: "LEFT OUTER JOIN LATERAL"
    defp join_qual(:right), do: "RIGHT OUTER JOIN"
    defp join_qual(:full), do: "FULL OUTER JOIN"

    defp index_expr(literal) when is_binary(literal), do: literal
    defp index_expr(literal), do: quote_name(literal)

    defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _query) do
      quote_qualified_name(field, sources, idx)
    end

    defp expr({:&, _, [idx, fields, _counter]}, sources, query) do
      {source, name, schema} = elem(sources, idx)

      if is_nil(schema) and is_nil(fields) do
        error!(
          query,
          "SnappyData requires a schema module when using selector " <>
            "#{inspect(name)} but none was given. " <>
            "Please specify a schema or specify exactly which fields from " <>
            "#{inspect(name)} you desire"
        )
      end

      Enum.map_join(fields, ", ", &"#{name}.#{&1}")
    end

    defp expr({:in, _, [_left, []]}, _sources, _query) do
      "false"
    end

    defp expr({:in, _, [left, right]}, sources, query) when is_list(right) do
      args = Enum.map_join(right, ",", &expr(&1, sources, query))
      expr(left, sources, query) <> " IN (" <> args <> ")"
    end

    defp expr({:in, _, [left, {:^, _, [ix, _]}]}, sources, query) do
      expr(left, sources, query) <> " = ANY($#{ix + 1})"
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
      [?(, all(query), ?)]
    end

    defp expr({:fragment, _, parts}, sources, query) do
      Enum.map_join(parts, "", fn
        {:raw, part} -> part
        {:expr, expr} -> expr(expr, sources, query)
      end)
    end

    defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

    #    defp expr(list, sources, query) when is_list(list) do
    #      "ARRAY[" <> Enum.map_join(list, ",", &expr(&1, sources, query)) <> "]"
    #    end

    defp expr(%Decimal{} = decimal, _sources, _query) do
      Decimal.to_string(decimal, :normal)
    end

    defp expr(%Ecto.Query.Tagged{value: binary, type: :binary}, _sources, _query)
         when is_binary(binary) do
      hex = Base.encode16(binary, case: :lower)
      "'\\x#{hex}'::bytea"
    end

    defp expr(nil, _sources, _query), do: "NULL"
    defp expr(true, _sources, _query), do: "TRUE"
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

    # At the very end

    defp expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
      {modifier, args} =
        case args do
          [rest, :distinct] -> {"DISTINCT ", [rest]}
          _ -> {[], args}
        end

      case handle_call(fun, length(args)) do
        {:binary_op, op} ->
          [left, right] = args
          [op_to_binary(left, sources, query), op | op_to_binary(right, sources, query)]

        {:fun, fun} ->
          [fun, ?(, modifier, intersperse_map(args, ", ", &expr(&1, sources, query)), ?)]
      end
    end

    defp op_to_binary({op, _, [_, _]} = expr, sources, query) when op in @binary_ops do
      paren_expr(expr, sources, query)
    end

    defp op_to_binary(expr, sources, query) do
      expr(expr, sources, query)
    end

    defp order_by_expr({dir, expr}, sources, query) do
      str = expr(expr, sources, query)

      case dir do
        :asc -> str
        :desc -> [str | " DESC"]
      end
    end

    defp quote_name(name) when is_atom(name) do
      quote_name(Atom.to_string(name))
    end

    defp quote_name(name) do
      if String.contains?(name, "\"") do
        error!(nil, "bad field name #{inspect(name)}")
      end

      [?", name, ?"]
    end

    defp quote_table(nil, name) do
      name
      |> quote_table
      |> String.upcase()
    end

    defp quote_table(prefix, name) do
      table = quote_table(prefix) <> "." <> quote_table(name)
      String.upcase(table)
    end

    defp quote_table(name) when is_atom(name), do: quote_table(Atom.to_string(name))

    defp quote_table(name) do
      if String.contains?(name, "\"") do
        error!(nil, "bad table name #{inspect(name)}")
      end

      name
    end

    defp intersperse_map(list, separator, mapper, acc \\ [])
    defp intersperse_map([], _separator, _mapper, acc), do: acc
    defp intersperse_map([elem], _separator, mapper, acc), do: [acc | mapper.(elem)]

    defp intersperse_map([elem | rest], separator, mapper, acc),
      do: intersperse_map(rest, separator, mapper, [acc, mapper.(elem), separator])

    defp options_expr(nil), do: ""

    defp options_expr(keyword) when is_list(keyword),
      do: error!(nil, "SnappyData adapter does not support keyword lists in :options")

    defp options_expr(options), do: " #{options}"

    ## DDL
    alias Ecto.Migration.{Table, Index, Reference, Constraint}

    @drops [:drop, :drop_if_exists]

    def execute_ddl({command, %Table{} = table, columns})
        when command in [:create, :create_if_not_exists] do
      options = options_expr(table.options)

      pk_definition =
        case pk_definition(columns) do
          nil -> ""
          pk -> ", #{pk}"
        end

      create_if_exists =
        if command == :create_if_not_exists do
          " IF NOT EXISTS"
        else
          ""
        end

      "CREATE TABLE" <>
        create_if_exists <>
        " #{quote_table(table.prefix, table.name)}" <>
        " (#{column_definitions(table, columns)}#{pk_definition})" <> options
    end

    def execute_ddl({:alter, %Table{} = table, changes}) do
      table_name = quote_table(table.prefix, table.name)

      query = [
        "ALTER TABLE ",
        table_name,
        ?\s,
        column_changes(table, changes),
        pk_definition(changes, ", ADD ")
      ]

      [query]
    end

    def execute_ddl({:create_if_not_exists, %Index{} = index}) do
      [execute_ddl({:create, index}), ";"]
    end

    def execute_ddl({:create, %Index{} = index}) do
      fields = Enum.map_join(index.columns, ", ", &index_expr/1)

      "CREATE #{if_do(index.unique, "UNIQUE ")}INDEX #{quote_name(index.name)} ON #{
        quote_table(index.prefix, index.table)
      } (#{fields})"
    end

    def execute_ddl({:create, %Constraint{} = constraint}) do
      "ALTER TABLE #{quote_table(constraint.prefix, constraint.table)} ADD #{
        new_constraint_expr(constraint)
      }"
    end

    defp comments_on(_object, _name, nil),
      do: error!(nil, "SnappyData adapter does not support comments")

    defp pk_definition(columns, prefix) do
      pks = for {_, name, _, opts} <- columns, opts[:primary_key], do: name

      case pks do
        [] -> []
        _ -> [prefix, "PRIMARY KEY (", intersperse_map(pks, ", ", &quote_name/1), ")"]
      end
    end

    defp new_constraint_expr(%Constraint{check: check} = constraint) when is_binary(check) do
      "CONSTRAINT #{quote_name(constraint.name)} CHECK (#{check})"
    end

    defp new_constraint_expr(%Constraint{exclude: exclude} = constraint)
         when is_binary(exclude) do
      "CONSTRAINT #{quote_name(constraint.name)} EXCLUDE USING #{exclude}"
    end

    defp pk_definition(columns) do
      pks = for {_, name, _, opts} <- columns, opts[:primary_key], do: name

      case pks do
        [] -> nil
        _ -> "PRIMARY KEY (" <> Enum.map_join(pks, ", ", &quote_name/1) <> ")"
      end
    end

    defp column_definitions(table, columns) do
      intersperse_map(columns, ", ", &column_definition(table, &1))
    end

    defp column_definition(table, {:add, name, %Reference{} = ref, opts}) do
      [
        [
          quote_name(name),
          ?\s,
          reference_column_type(ref.type, opts),
          column_options(ref.type, opts),
          reference_expr(ref, table, name)
        ]
      ]
    end

    defp column_definition(_table, {:add, name, type, opts}) do
      [[quote_name(name), ?\s, column_type(type, opts), column_options(type, opts)]]
    end

    defp reference_column_type(:serial, _opts), do: "BIGINT"
    defp reference_column_type(type, opts), do: column_type(type, opts)

    defp modify_null(name, opts) do
      case Keyword.get(opts, :null) do
        true -> [", ALTER COLUMN ", quote_name(name), " DROP NOT NULL"]
        false -> [", ALTER COLUMN ", quote_name(name), " SET NOT NULL"]
        nil -> []
      end
    end

    defp modify_default(name, type, opts) do
      case Keyword.fetch(opts, :default) do
        {:ok, val} ->
          [", ALTER COLUMN ", quote_name(name), " SET", default_expr({:ok, val}, type)]

        :error ->
          []
      end
    end

    defp column_options(type, opts) do
      default = Keyword.fetch(opts, :default)
      null = Keyword.get(opts, :null)
      [default_expr(default, type), null_expr(null)]
    end

    defp column_changes(table, columns) do
      intersperse_map(columns, ", ", &column_change(table, &1))
    end

    defp column_change(table, {:add, name, %Reference{} = ref, opts}) do
      [
        "ADD COLUMN ",
        quote_name(name),
        ?\s,
        reference_column_type(ref.type, opts),
        column_options(ref.type, opts),
        reference_expr(ref, table, name)
      ]
    end

    defp column_change(_table, {:add, name, type, opts}) do
      ["ADD COLUMN ", quote_name(name), ?\s, column_type(type, opts), column_options(type, opts)]
    end

    defp column_change(table, {:modify, name, %Reference{} = ref, opts}) do
      [
        "ALTER COLUMN ",
        quote_name(name),
        " TYPE ",
        reference_column_type(ref.type, opts),
        constraint_expr(ref, table, name),
        modify_null(name, opts),
        modify_default(name, ref.type, opts)
      ]
    end

    defp column_change(_table, {:modify, name, type, opts}) do
      [
        "ALTER COLUMN ",
        quote_name(name),
        " TYPE ",
        column_type(type, opts),
        modify_null(name, opts),
        modify_default(name, type, opts)
      ]
    end

    defp column_change(_table, {:remove, name}), do: ["DROP COLUMN ", quote_name(name)]

    defp reference_name(%Reference{name: nil}, table, column),
      do: quote_name("#{table.name}_#{column}_fkey")

    defp reference_name(%Reference{name: name}, _table, _column), do: quote_name(name)

    # Foreign key definition
    defp reference_expr(%Reference{} = ref, table, name),
      do: [
        " CONSTRAINT ",
        reference_name(ref, table, name),
        ?\s,
        "REFERENCES ",
        quote_table(table.prefix, ref.table),
        ?(,
        quote_name(ref.column),
        ?),
        reference_on_delete(ref.on_delete),
        reference_on_update(ref.on_update)
      ]

    defp constraint_expr(%Reference{} = ref, table, name),
      do: [
        ", ADD CONSTRAINT ",
        reference_name(ref, table, name),
        ?\s,
        "FOREIGN KEY (",
        quote_name(name),
        ") REFERENCES ",
        quote_table(table.prefix, ref.table),
        ?(,
        quote_name(ref.column),
        ?),
        reference_on_delete(ref.on_delete),
        reference_on_update(ref.on_update)
      ]

    defp column_type({:array, type}, opts), do: column_type(type, opts) <> "[]"

    defp column_type(type, opts) do
      size = Keyword.get(opts, :size)
      precision = Keyword.get(opts, :precision)
      scale = Keyword.get(opts, :scale)
      type_name = ecto_to_db(type)

      cond do
        size -> "#{type_name}(#{size})"
        precision -> "#{type_name}(#{precision},#{scale || 0})"
        type == :string -> "#{type_name}(255)"
        true -> "#{type_name}"
      end
    end

    defp reference_on_delete(:restrict_all), do: " ON DELETE RESTRICT"
    defp reference_on_delete(_), do: ""

    defp reference_on_update(_), do: ""

    defp default_expr({:ok, nil}, _type), do: " DEFAULT NULL"

    defp default_expr({:ok, literal}, _type) when is_binary(literal),
      do: " DEFAULT '#{escape_string(literal)}'"

    defp default_expr({:ok, literal}, _type) when is_number(literal) or is_boolean(literal),
      do: " DEFAULT #{literal}"

    defp default_expr({:ok, {:fragment, expr}}, _type), do: " DEFAULT #{expr}"

    defp default_expr({:ok, expr}, type),
      do:
        raise(
          ArgumentError,
          "unknown default `#{inspect(expr)}` for type `#{inspect(type)}`. " <>
            ":default may be a string, number, boolean, empty list or a fragment(...)"
        )

    defp default_expr(:error, _), do: []

    defp null_expr(false), do: " NOT NULL"
    defp null_expr(true), do: " NULL"
    defp null_expr(_), do: []

    ## Helpers

    defp quote_qualified_name(name, sources, ix) do
      {_, source, _} = elem(sources, ix)
      [source, ?. | quote_name(name)]
    end

    defp if_do(condition, value) do
      if condition, do: value, else: []
    end

    defp get_source(query, sources, ix, source) do
      {expr, name, _schema} = elem(sources, ix)
      {expr || expr(source, sources, query), name}
    end

    defp escape_string(value) when is_binary(value) do
      :binary.replace(value, "'", "''", [:global])
    end

    defp intersperse_reduce(list, separator, user_acc, reducer, acc \\ [])
    defp intersperse_reduce([], _separator, user_acc, _reducer, acc), do: {acc, user_acc}

    defp intersperse_reduce([elem], _separator, user_acc, reducer, acc) do
      {elem, user_acc} = reducer.(elem, user_acc)
      {[acc | elem], user_acc}
    end

    defp intersperse_reduce([elem | rest], separator, user_acc, reducer, acc) do
      {elem, user_acc} = reducer.(elem, user_acc)
      intersperse_reduce(rest, separator, user_acc, reducer, [acc, elem, separator])
    end

    defp error!(nil, message) do
      raise ArgumentError, message
    end

    defp error!(query, message) do
      raise Ecto.QueryError, query: query, message: message
    end

    defp ecto_to_db(:id), do: "BIGINT"
    defp ecto_to_db(:binary_id), do: "CHAR(36)"
    defp ecto_to_db(:string), do: "VARCHAR"
    defp ecto_to_db(:utc_datetime, _query), do: "TIMESTAMP"
    defp ecto_to_db(:naive_datetime), do: "TIMESTAMP"
    defp ecto_to_db(:boolean), do: "SMALLINT"
    defp ecto_to_db(:binary), do: "BLOB"
    defp ecto_to_db(:text), do: "STRING"
    defp ecto_to_db(:uuid), do: "CHAR(36)"
    # @TODO
    # defp ecto_to_db(:map), do: "STRING"
    # @TODO
    # defp ecto_to_db({:map, _}), do: "STRING"
    defp ecto_to_db(:serial), do: "INTEGER"
    defp ecto_to_db(:bigserial), do: "BIGINT"
    defp ecto_to_db(other), do: Atom.to_string(other)
  end
end
