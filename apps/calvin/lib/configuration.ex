# Module for managing a Calvin deployment configuration. Every component
# can store a basic instance of Configuration to be aware of the number
# of replicas and / or partitions in the Calvin deployment via a PartitionScheme 
# or a type of ReplicationScheme, and can call utility functions for generating
# views of relevant components.

defmodule Configuration do
  alias __MODULE__

  @enforce_keys [:replication_scheme, :partition_scheme]

  defstruct(
    replication_scheme: nil,
    partition_scheme: nil
  )

  @doc """
  Creates a new Configuration with `partition_scheme` for partitioning 
  configuration and `replication_scheme` for managing replication. Supports
  both async replication and synchronous Raft-based replication schemes
  """
  @spec new(%ReplicationScheme.Async{} | %ReplicationScheme.Raft{}, %PartitionScheme{}) :: %Configuration{}
  def new(replication_scheme, partition_scheme) do
    %Configuration {
      replication_scheme: replication_scheme,
      partition_scheme: partition_scheme
    }
  end

  @doc """
  Given a Configuration, returns which type of replication scheme the given configuration
  is using - `async` or `raft`
  """
  @spec using_replication?(%Configuration{}) :: atom()
  def using_replication?(configuration) do
    case configuration.replication_scheme do
      %ReplicationScheme.Async{} ->
        :async
      %ReplicationScheme.Raft{} ->
        :raft
    end
  end

  @doc """
  Returns a list view of partitions per replica in the given Configuration's PartitionScheme
  """
  @spec get_partition_view(%Configuration{}) :: [non_neg_integer()]
  def get_partition_view(configuration) do
    PartitionScheme.get_partition_view(configuration.partition_scheme)
  end
  
  @doc """
  Returns a list view of replicas in the given Configuration's ReplicationScheme
  """
  @spec get_replica_view(%Configuration{}) :: [atom()]
  def get_replica_view(configuration) do
    ReplicationScheme.get_replica_view(configuration.replication_scheme)
  end

  @doc """
  Returns a list view of Storage components for a replica in the given Configuration
  """
  @spec get_storage_view(%Configuration{}, atom()) :: [atom()]
  def get_storage_view(configuration, replica) do
    partitions = Configuration.get_partition_view(configuration)
    Enum.map(partitions, 
      fn partition ->
        Component.id(_replica=replica, _partition=partition, _type=:storage)
      end
    )
  end

  @doc """
  Returns a list view of Sequencer components for a replica in the given Configuration
  """
  @spec get_sequencer_view(%Configuration{}, atom()) :: [atom()]
  def get_sequencer_view(configuration, replica) do
    partitions = Configuration.get_partition_view(configuration)
    Enum.map(partitions,
      fn partition ->
        Component.id(_replica=replica, _partition=partition, _type=:sequencer)
      end
    )
  end
end