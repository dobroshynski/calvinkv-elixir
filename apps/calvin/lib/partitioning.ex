# Module for managing the partitioning configuration for a Calvin
# deployment

defmodule PartitionScheme do
  alias __MODULE__

  @enforce_keys [:num_partitions]

  defstruct(
    num_partitions: nil
  )

  @doc """
  Creates a new PartitionScheme with `num_partitions` partitions per replica
  """
  @spec new(non_neg_integer()) :: %PartitionScheme{}
  def new(num_partitions) do
    %PartitionScheme{
      num_partitions: num_partitions,
    }
  end

  @doc """
  Returns a list view of partitions per replica in the given PartitionScheme
  """
  @spec get_partition_view(%PartitionScheme{}) :: [non_neg_integer()]
  def get_partition_view(partition_scheme) do
    partion_range = 1..partition_scheme.num_partitions
    Enum.to_list partion_range
  end

  @doc """
  Returns a list of partitions other than the partition of a given component / process `proc`,
  given a PartitionScheme
  """
  @spec get_all_other_partitions(%Storage{} | %Sequencer{} | %Scheduler{}, %PartitionScheme{}) :: [non_neg_integer()]
  def get_all_other_partitions(proc, partition_scheme) do
    partitions = PartitionScheme.get_partition_view(partition_scheme)
    Enum.filter(partitions, fn partition -> partition != proc.partition end)
  end
end