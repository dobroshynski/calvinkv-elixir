# Module for managing an asynchronous mode of replication in a Calvin deployment. Provides
# functions for managing which replica is designated as the main replica that is in charge
# of replicating the Transaction batch input

defmodule AsyncReplicationScheme do
  alias __MODULE__

  @enforce_keys [:num_replicas]

  defstruct(
    num_replicas: nil,
    # for async replication, storing which replica is the 
    # main replica
    main_replica: nil
  )

  @doc """
  Creates a new AsyncReplicationScheme with `num_replicas` replicas and `main_replica`
  as the main replica of the system deployment with async replication
  """
  @spec new(non_neg_integer(), atom()) :: %AsyncReplicationScheme{}
  def new(num_replicas, main_replica) do
    %AsyncReplicationScheme{
      num_replicas: num_replicas,
      main_replica: main_replica
    }
  end

  @doc """
  Creates a new AsyncReplicationScheme with `num_replicas` replicas
  """
  @spec new(non_neg_integer()) :: %AsyncReplicationScheme{}
  def new(num_replicas) do
    replication_scheme = %AsyncReplicationScheme{
      num_replicas: num_replicas
    }

    # if no main replica was provided, default to the 
    # name of the first replica
    replicas = ReplicationScheme.get_replica_view(replication_scheme)
    AsyncReplicationScheme.set_main_replica(replication_scheme, _replica=Enum.at(replicas, 0))
  end

  @doc """
  Updates the main replica for a given AsyncReplicationScheme 
  """
  @spec set_main_replica(%AsyncReplicationScheme{}, atom()) :: %AsyncReplicationScheme{}
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
  @spec get_replica_view(%AsyncReplicationScheme{}) :: [atom()]
  def get_replica_view(replication_scheme) do
    max_replica = replication_scheme.num_replicas - 1
    replica_range = 0..max_replica
    # 'A' is 65 codepoint, so we use that to convert 0,1,2 -> :A,:B,:C and so on
    Enum.map(replica_range, fn n -> List.to_atom([n + 65]) end)
  end

  @doc """
  Returns a list of replicas other than the replica of a given component / process `proc`,
  given an AsyncReplicationScheme
  """
  @spec get_all_other_replicas(%Storage{} | %Sequencer{} | %Scheduler{}, %AsyncReplicationScheme{}) :: [atom()]
  def get_all_other_replicas(proc, replication_scheme) do
    replicas = ReplicationScheme.get_replica_view(replication_scheme)
    Enum.filter(replicas, fn replica -> replica != proc.replica end)
  end
end