defmodule Debug do

  @debug false
  @debug_raft false
  @experiments false

  import Emulation, only: [whoami: 0]

  @doc """
  Logs the string along with process id
  """
  @spec log(charlist()) :: no_return()
  def log(string) do
    if @debug do
      IO.puts("[node #{whoami()}] #{string}")
    end
  end

  @doc """
  Logs the string along with the process Raft state
  role and process id
  """
  @spec log(charlist(), charlist()) :: no_return()
  def log(string, raft_role) do
    if @debug_raft do
      IO.puts("[node #{whoami()}][#{raft_role}] #{string}")
    end
  end
end
