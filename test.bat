set MIX_ENV=snappydata
set SNAPPYDATA_PORT=1528
set SNAPPYDATA_HOST=192.168.0.23
set TZ=Etc/UTC
REM mix test deps/snappyex/test/login_test.exs:16 || true
mix test test/snappydata_ecto_test.exs --seed 0 REM --trace
