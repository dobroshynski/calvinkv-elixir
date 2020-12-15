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

# Module with utility functions for managing testing

defmodule Testing do

  import Emulation, only: [send: 2]
  import Kernel, except: [send: 2]

  @doc """
  Helper function to collect the key-value store states of Storage components given 
  a list of Storage unique ids
  """
  @spec get_kv_stores([atom()]) :: [%{}]
  def get_kv_stores(storage_ids) do
    Enum.map(storage_ids,
      fn id ->
        # send testing / debug message to the Storage component directly
        send(id, :get_kv_store)
      end
    )
    Enum.map(storage_ids,
      fn id ->
        receive do
          {^id, kv_store} -> kv_store
        end
      end
    )
  end

  @doc """
  Helper function to collect the states of Sequencer components given 
  a list of Sequencer unique ids
  """
  @spec get_sequencer_states([atom()]) :: [%Sequencer{}]
  def get_sequencer_states(sequencer_ids) do
    Enum.map(sequencer_ids,
      fn id ->
        # send testing / debug message to the Sequencer component directly
        send(id, :get_state)
      end
    )
    Enum.map(sequencer_ids,
      fn id ->
        receive do
          {^id, state} -> state
        end
      end
    )
  end
end