export MIX_ENV=snappydata
export SNAPPYDATA_PORT=31320
export SNAPPYDATA_HOST="192.168.0.23"
export TZ=Etc/UTC
#mix test deps/snappyex/test/login_test.exs:16 || true
mix test test/snappydata_ecto_test.exs --seed 0 # --trace
