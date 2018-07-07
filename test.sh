export MIX_ENV=snappydata
export SNAPPYDATA_PORT=1528
export SNAPPYDATA_HOST="192.168.0.10"
export TZ=Etc/UTC
mix test deps/snappyex/test/login_test.exs:16 || true
#mix test test/snappydata_ecto_test.exs --seed 0 # --trace
#mix test integration_test/snappydata/storage_test.exs
