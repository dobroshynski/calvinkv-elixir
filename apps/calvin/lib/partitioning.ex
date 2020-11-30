# Module for managing the partitioning configuration for a Calvin
# deployment

defmodule PartitionScheme do
  alias __MODULE__

  @enforce_keys [:num_partitions, :key_partition_map]

  defstruct(
    num_partitions: nil,
    # mapping of key to partition which the key is assigned to
    key_partition_map: nil
  )

  @doc """
  Generates a key partition map given `num_partitions` of partitions. This maps 
  each value in a key range from a -> z to an assigned partition in the range [1, num_partitions],
  and in a way 'chunks' the key range such that it is split between partitions. 

  For example, given `num_partitions` = 2, the key range of [a ... z] is first partitioned into
  [[a ... m], [n ... z]]. Then it is flat mapped into a list of maps in form of 
  [{a->1}, {b->1} ... {z->2}] with keys a-m assigned to partition 1 and n-z assigned to partition 2.
  Then the maps are combined into a single map of {key -> partition} such that for each key in the 
  range of a->z, based on how many partitions we have, we have a corresponding partition assigned
  """
  @spec generate_key_partition_map(non_neg_integer()) :: %{}
  def generate_key_partition_map(num_partitions) do
    # generate a range of keys from a -> z
    key_range = Enum.map(0..25, fn n -> List.to_atom([n + 97]) end)

    # partition the key range such that we have `num_partitions` chunks
    key_range_partitioned = Enum.chunk_every(key_range, ceil(26 / num_partitions))
  
    # index each chunk, then map each chunk such that each key in the chunk
    # is paired with the index of the chunk. This correspondings to assigning
    # a partition number to each value in the key range and wil result in multiple
    # maps which are then flattened into a single list
    key_maps = Enum.flat_map(Enum.with_index(key_range_partitioned), 
      fn {range, idx} -> 
        Enum.map(range, 
          fn val -> 
            count = %{}
            Map.put(count, val, idx+1)
          end) 
      end
    )
    # reduce the maps of key -> partition into a single map
    Enum.reduce(key_maps, %{}, fn map, acc -> Map.merge(acc, map) end)
  end


  @doc """
  Creates a new PartitionScheme with `num_partitions` partitions per replica
  """
  @spec new(non_neg_integer()) :: %PartitionScheme{}
  def new(num_partitions) do
    key_partition_map = PartitionScheme.generate_key_partition_map(num_partitions)
    
    %PartitionScheme{
      num_partitions: num_partitions,
      key_partition_map: key_partition_map
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