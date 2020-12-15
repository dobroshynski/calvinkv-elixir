# Module for managing the partitioning configuration for a Calvin
# deployment

defmodule PartitionScheme do
  alias __MODULE__

  @enforce_keys [:num_partitions, :partition_key_map]

  defstruct(
    num_partitions: nil,
    # mapping of key to partition which the key is assigned to
    partition_key_map: nil
  )

  @doc """
  Given a value and a partition, returns whether the given value is local to the given
  partition based on the given partition scheme

  For example, assuming a PartitionScheme with 2 partitions, value `a` is local to partition 1 
  while value `z` is local to partition 2, since the `partition_key_map` ranges are [a->m] and [n->z]
  for partitions 1 and 2, respectively
  """
  @spec is_local?(atom(), non_neg_integer(), %PartitionScheme{}) :: boolean()
  def is_local?(val, partition, partition_scheme) do
    stored_on_partition = Map.get(partition_scheme.partition_key_map, PartitionScheme.to_partition_key(_value=val))
    if stored_on_partition == partition do
      true
    else
      false
    end
  end

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
  @spec generate_partition_key_map(non_neg_integer()) :: %{}
  def generate_partition_key_map(num_partitions) do
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
  Converts any key of type atom() to a partition key, which is in the range of [a -> z]
  """
  @spec to_partition_key(atom()) :: atom()
  def to_partition_key(val) do
    # convert the atom to string, then take the first character and convert it to lowercase
    key_string = String.downcase(String.at(to_string(val), 0))
    # convert the string back to atom
    String.to_atom(key_string)
  end

  @doc """
  Given a batch of Transactions with their read/write sets generated, returns a map of
  partitioned Transactions in the form of {partition number -> Transaction batch for that partition}
  """
  @spec partition_transactions([%Transaction{}]) :: %{}
  def partition_transactions(tx_batch) do
    # iterate the batch of all Transactions and reduce into a map of {partition -> Transactions}
    Enum.reduce(tx_batch, %{}, 
      fn tx, acc ->
        # get the participating partitions for this Transaction and update the map for each
        # partition
        partitions = tx.participating_partitions
        
        # update the map of {partition -> Transactions} for each participating partition
        Enum.reduce(partitions, acc, 
          fn partition, updated_map -> 
            Map.put(updated_map, partition, Map.get(updated_map, partition, []) ++ [tx])
          end
        )
      end
    )
  end

  @doc """
  Given a batch of Transactions and a PartitionScheme, returns an updated batch of Transactions
  with each Transaction's set of participating partitions generated according to the Transaction's
  read and write sets
  """
  @spec generate_participating_partitions([%Transaction{}], %PartitionScheme{}) :: [%Transaction{}]
  def generate_participating_partitions(tx_batch, partition_scheme) do
    Enum.map(tx_batch,
      fn tx ->
        # get the set of active participating partitions, which is the set
        # of partitions on which the values in the current Transaction are being
        # either created, updated, or deleted
        active_participants = Enum.reduce(tx.write_set, MapSet.new(),
          fn value, acc ->
            # get the partition that this value being written to is on
            partition = Map.get(partition_scheme.partition_key_map, to_partition_key(_value=value))
            # add to the set of partitions
            MapSet.put(acc, partition)
          end
        )

        # get the set of passive participating partitions, which is the set
        # of partitions on which only READ operations are performed
        passive_participants = Enum.reduce(tx.read_set, MapSet.new(),
          fn value, acc ->
            # get the partition that this value being read is on
            partition = Map.get(partition_scheme.partition_key_map, to_partition_key(_value=value))
            # add to the set of partitions
            MapSet.put(acc, partition)
          end
        )

        # combine the read and write sets for the Transaction to get all
        # of the values that the Transactions accesses and convert to list form
        all_participants = MapSet.union(passive_participants, active_participants)

        # update the participating partitions for the Transaction
        Transaction.set_participating_partitions(
          _transaction=tx,
          _active=active_participants, 
          _passive=passive_participants,
          _all=all_participants
        )
      end)
  end

  @doc """
  Creates a new PartitionScheme with `num_partitions` partitions per replica and generates a
  partition key map in order to associate Transactions with partitions
  """
  @spec new(non_neg_integer()) :: %PartitionScheme{}
  def new(num_partitions) do
    partition_key_map = PartitionScheme.generate_partition_key_map(num_partitions)
  
    %PartitionScheme{
      num_partitions: num_partitions,
      partition_key_map: partition_key_map
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