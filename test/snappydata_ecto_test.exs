defmodule Snappydata.Ecto.Test do
  use ExUnit.Case
  doctest Snappydata.Ecto

  import Ecto.Query

  alias Ecto.Queryable
  alias Ecto.Adapters.SnappyData.Connection, as: SQL

  defmodule Schema do
    use Ecto.Schema

    schema "schema" do
      field :x, :integer
      field :y, :integer
      field :z, :integer
      field :w, {:array, :integer}

      has_many :comments, Ecto.Adapters.SnappyData.Test.Schema2,
        references: :x,
        foreign_key: :z
      has_one :permalink, Ecto.Adapters.SnappyData.Test.Schema3,
        references: :y,
        foreign_key: :id
    end
  end

  defmodule Schema2 do
    use Ecto.Schema

    schema "schema2" do
      belongs_to :post, Ecto.Adapters.SnappyData.Test.Schema,
        references: :x,
        foreign_key: :z
    end
  end

  defmodule Schema3 do
    use Ecto.Schema

    schema "schema3" do
      field :list1, {:array, :string}
      field :list2, {:array, :integer}
      field :binary, :binary
    end
  end

  defp normalize(query, operation \\ :all, counter \\ 0) do
    {query, _params, _key} = Ecto.Query.Planner.prepare(query, operation, Ecto.Adapters.SnappyData, counter)
    Ecto.Query.Planner.normalize(query, operation, Ecto.Adapters.SnappyData, counter)
  end

  test "from" do
    query = Schema |> select([r], r.x) |> normalize
    assert SQL.all(query) == ~s{SELECT s0.X FROM SCHEMA AS s0}
  end

  test "from without schema" do
    query = "posts" |> select([r], r.x) |> normalize
    assert SQL.all(query) == ~s{SELECT p0.X FROM POSTS AS p0}

    query = "posts" |> select([:x]) |> normalize
    assert SQL.all(query) == ~s{SELECT p0.X FROM POSTS AS p0}

    assert_raise Ecto.QueryError, ~r"SnappyData requires a schema module when using selector \"p0\" but none was given. Please specify a schema or specify exactly which fields from \"p0\" you desire in query:\n\nfrom p in \"posts\",\n  select: p\n", fn ->
      SQL.all from(p in "posts", select: p) |> normalize()
    end
  end

  test "the truth" do
    assert 1 + 1 == 2
  end
end
