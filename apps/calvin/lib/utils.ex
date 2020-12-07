# Module with utility functions for managing debugging

defmodule Debug do

  @debug false
  @debug_raft false

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

  defmodule Timing do
    @debug_timing true

    @doc """
    Logs the string when running timing experiments
    """
    @spec log(charlist()) :: no_return()
    def log(string) do
      if @debug_timing do
        IO.puts("[timing] #{string}")
      end
    end
  end
end
