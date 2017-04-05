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

#      has_many :comments, Ecto.Adapters.PostgresTest.Schema2,
#        references: :x,
#        foreign_key: :z
#      has_one :permalink, Ecto.Adapters.PostgresTest.Schema3,
#        references: :y,
#        foreign_key: :id
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

  test "the truth" do
    assert 1 + 1 == 2
  end
end
