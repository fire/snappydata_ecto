# For tasks/generators testing
Mix.start()
Mix.shell(Mix.Shell.Process)
System.put_env("ECTO_EDITOR", "")
Logger.configure(level: :info)

Code.require_file("../../deps/ecto/integration_test/support/file_helpers.exs", __DIR__)
ExUnit.start()
