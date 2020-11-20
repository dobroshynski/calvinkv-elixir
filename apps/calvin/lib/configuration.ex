# Module for managing a Calvin deployment configuration. Every component
# can store a basic instance of Configuration to be aware of the number
# of replicas and / or partitions in the Calvin deployment and can 
# call utility functions for generating views of relevant components.

defmodule Configuration do
  @enforce_keys [:num_replicas, :num_partitions]

  defstruct(
    num_replicas: nil,
    num_partitions: nil
  )

  @doc """
  Creates a new Configuration with `num_replicas` replicas and `num_partitions`
  partitions per replica
  """
  @spec new(non_neg_integer(), non_neg_integer()) :: %Configuration{}
  def new(num_replicas, num_partitions) do
    %Configuration{
      num_replicas: num_replicas,
      num_partitions: num_partitions
    }
  end

  @doc """
  Returns a list view of partitions per replica in the given Configuration
  """
  @spec get_partition_view(%Configuration{}) :: [non_neg_integer()]
  def get_partition_view(configuration) do
    partion_range = 1..configuration.num_partitions
    Enum.to_list partion_range
  end
  
  @doc """
  Returns a list view of replicas in the given Configuration
  """
  @spec get_replica_view(%Configuration{}) :: [atom()]
  def get_replica_view(configuration) do
    max_replica = configuration.num_replicas - 1
    replica_range = 0..max_replica
    # 'A' is 65 codepoint, so we use that to convert 0,1,2 -> :A,:B,:C and so on
    Enum.map(replica_range, fn n -> List.to_atom([n + 65]) end)
  end

  @doc """
  Returns a list of partitions other than the partition of a given component / process `proc`,
  given a Configuration
  """
  @spec get_all_other_partitions(%Storage{} | %Sequencer{} | %Scheduler{}, %Configuration{}) :: [non_neg_integer()]
  def get_all_other_partitions(proc, configuration) do
    partitions = Configuration.get_partition_view(configuration)
    Enum.filter(partitions, fn partition -> partition != proc.partition end)
  end

  @doc """
  Returns a list of replicas other than the replica of a given component / process `proc`,
  given a Configuration
  """
  @spec get_all_other_replicas(%Storage{} | %Sequencer{} | %Scheduler{}, %Configuration{}) :: [atom()]
  def get_all_other_replicas(proc, configuration) do
    replicas = Configuration.get_replica_view(configuration)
    Enum.filter(replicas, fn replica -> replica != proc.replica end)
  end
end