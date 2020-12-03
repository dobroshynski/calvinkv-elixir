# Module for managing a syncronous Raft-based mode of replication in a Calvin deployment. 
# Provides functions and sub-modules for managing Raft messanging, logs, commits, and other
# coordination
# 
# Partitions are organized into replication groups, with every replication group running
# Raft to synchronously replicate batches of Transactions

defmodule ReplicationScheme.Raft do
  @enforce_keys [:num_replicas, :current_leaders]

  defstruct(
    num_replicas: nil,
    # for Raft-based replication, storing which replica
    # is the leader for every replication group
    # {partition -> leader replica}
    current_leaders: nil
  )

  @doc """
  Creates a new Raft ReplicationScheme with `num_replicas` replicas and initializes
  the current leaders for each partition to the default replica based on the number
  of given `num_partitions`
  """
  @spec new(non_neg_integer(), non_neg_integer()) :: %ReplicationScheme.Raft{}
  def new(num_replicas, num_partitions) do
    initial_leaders = Enum.reduce(1..num_partitions, %{}, 
      fn partition, acc ->
        # initially all partition groups have replica A as the leader
        Map.put(acc, partition, :A)
      end
    )
    %ReplicationScheme.Raft{
      num_replicas: num_replicas,
      current_leaders: initial_leaders
    }
  end

  @doc """
  Updates the current leader replica for a given partition, the participants of which across
  replicas make up a replication group
  """
  @spec set_leader_for_partition(%ReplicationScheme.Raft{}, non_neg_integer(), atom()) :: %ReplicationScheme.Raft{}
  def set_leader_for_partition(replication_scheme, partition, leader) do
    %{replication_scheme | 
      current_leaders: Map.put(replication_scheme.current_leaders, partition, leader)
    }
  end
end

# Module for managing an asynchronous mode of replication in a Calvin deployment. Provides
# functions for managing which replica is designated as the main replica that is in charge
# of replicating the Transaction batch input

defmodule ReplicationScheme.Async do
  @enforce_keys [:num_replicas]

  defstruct(
    num_replicas: nil,
    # for async replication, storing which replica is the 
    # main replica
    main_replica: nil
  )

  @doc """
  Creates a new Async ReplicationScheme with `num_replicas` replicas and `main_replica`
  as the main replica of the system deployment with async replication
  """
  @spec new(non_neg_integer(), atom()) :: %ReplicationScheme.Async{}
  def new(num_replicas, main_replica) do
    %ReplicationScheme.Async{
      num_replicas: num_replicas,
      main_replica: main_replica
    }
  end

  @doc """
  Creates a new Async ReplicationScheme with `num_replicas` replicas
  """
  @spec new(non_neg_integer()) :: %ReplicationScheme.Async{}
  def new(num_replicas) do
    replication_scheme = %ReplicationScheme.Async{
      num_replicas: num_replicas
    }

    # if no main replica was provided, default to the 
    # name of the first replica
    replicas = ReplicationScheme.get_replica_view(replication_scheme)
    ReplicationScheme.Async.set_main_replica(replication_scheme, _replica=Enum.at(replicas, 0))
  end

  @doc """
  Updates the main replica for a given Async ReplicationScheme 
  """
  @spec set_main_replica(%ReplicationScheme.Async{}, atom()) :: %ReplicationScheme.Async{}
  def set_main_replica(replication_scheme, replica) do
    %{replication_scheme | main_replica: replica}
  end
end

# Module for utility functions used across ReplicationSchemes

defmodule ReplicationScheme do
  alias __MODULE__

  @doc """
  Returns a list view of replicas in a given ReplicationScheme
  """
  @spec get_replica_view(%ReplicationScheme.Async{} | %ReplicationScheme.Raft{}) :: [atom()]
  def get_replica_view(replication_scheme) do
    max_replica = replication_scheme.num_replicas - 1
    replica_range = 0..max_replica
    # 'A' is 65 codepoint, so we use that to convert 0,1,2 -> :A,:B,:C and so on
    Enum.map(replica_range, fn n -> List.to_atom([n + 65]) end)
  end

  @doc """
  Returns a list of replicas other than the replica of a given component / process `proc`,
  given either an Async or Raft ReplicationScheme
  """
  @spec get_all_other_replicas(%Storage{} | %Sequencer{} | %Scheduler{}, %ReplicationScheme.Async{} | %ReplicationScheme.Raft{}) :: [atom()]
  def get_all_other_replicas(proc, replication_scheme) do
    replicas = ReplicationScheme.get_replica_view(replication_scheme)
    Enum.filter(replicas, fn replica -> replica != proc.replica end)
  end
end