# snappydata_ecto

Experimental Snappydata driver. Requires more QA before use in production.

Requires SnappyData to be launched in Thrift's framed mode.

See: 

> thrift-framed-transport=(true|false): to use the thrift framed transport; this is not the recommended mode since it provides no advantages over the default with SnappyData's server implementation but has been provided for languages that only support framed transport

https://github.com/SnappyDataInc/snappydata/blob/master/cluster/README-thrift.md

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `snappydata_ecto` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:snappydata_ecto, "~> 0.1.0"}]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/snappydata_ecto](https://hexdocs.pm/snappydata_ecto).

## Integration Testing

export MIX_ENV=snappydata
export SNAPPYDATA_HOST="192.168.0.23"
export SNAPPYDATA_PORT=32254
mix test
