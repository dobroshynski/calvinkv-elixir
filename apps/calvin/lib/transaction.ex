# Simple CRUD Transaction Operation module. This represents a simple 
# operation that can performed against a Storage component that is a
# key-value store

defmodule Transaction.Op do
  @enforce_keys [:type]

  defstruct(
    type: nil,
    key: nil,
    val: nil
  )

  @doc """
  Returns a condensed representation of an Operation, which is a tuple of
  the Operation type and either just the key, or the key and the value 
  associated with the Operation
  """
  @spec condensed(%Transaction.Op{}) :: %Transaction.Op{}
  def condensed(op) do
    case op.type do
      type = :READ ->
        {type, op.key}
      
      type = :CREATE ->
        {type, op.key, op.val}
      
      type = :UPDATE ->
        {type, op.key, op.val}

      type = :DELETE ->
        {type, op.key}
    end
  end

  @doc """
  Given a `Transaction.Op` Transaction operation, a partition, and a PartitionScheme, returns whether 
  the operation is local to the given `partition` based on the partition key converted from the operation
  key
  
  For example, assuming a PartitionScheme with 2 partitions, an operation of CREATE {a -> 1} is local
  to partition 1 while operation UPDATE {z -> 0} is local to partition 2, since the `partition_key_map`
  ranges are [a->m] and [n->z] for partitions 1 and 2, respectively
  """
  @spec is_local_to_partition?(%Transaction.Op{}, non_neg_integer(), %PartitionScheme{}) :: boolean()
  def is_local_to_partition?(op, partition, partition_scheme) do
    # convert whatever the key for the operation is to a partition key and
    # use the partition key map in the PartitionScheme to look up the partition
    local_op_partition = Map.get(partition_scheme.partition_key_map, PartitionScheme.to_partition_key(_value=op.key))
    if local_op_partition == partition do
      true
    else
      false
    end
  end

  @doc """
  CREATE Operation
  """
  @spec create(any(), any()) :: %Transaction.Op{}
  def create(key, val) do
    %Transaction.Op{
      type: :CREATE,
      key: key,
      val: val
    }
  end

  @doc """
  READ Operation
  """
  @spec read(any()) :: %Transaction.Op{}
  def read(key) do
    %Transaction.Op{
      type: :READ,
      key: key
    }
  end

  @doc """
  UPDATE Operation
  """
  @spec update(any(), any()) :: %Transaction.Op{}
  def update(key, val) do
    %Transaction.Op{
      type: :UPDATE,
      key: key,
      val: val
    }
  end

  @doc """
  DELETE Operation
  """
  @spec delete(any()) :: %Transaction.Op{}
  def delete(key) do
    %Transaction.Op{
      type: :DELETE,
      key: key
    }
  end
end

# Module representing a Transaction in the system. A single Transaction
# consists of a series of `Transaction.Op` operations that are executed
# against a Storage component

defmodule Transaction do
  alias __MODULE__

  @enforce_keys [:operations, :read_set, :write_set]

  defstruct(
    operations: nil,
    timestamp: nil,

    # read and write sets of the Transaction
    read_set: nil,
    write_set: nil,

    # participating partitions for this Transaction
    participating_partitions: nil,

    # for timing of Transaction start / execution from
    # client to Storage
    started: nil,
    finished: nil,
    duration: nil,

    # optional Transaction id for debugging purposes
    id: nil
  )

  @doc """
  Sets a timestamp to when this Transaction was started, being sent
  by a client to a Sequencer component
  """
  @spec set_started(%Transaction{}, non_neg_integer()) :: %Transaction{}
  def set_started(tx, time) do
    %{tx | started: time}
  end

  @doc """
  Sets a timestamp to when this Transaction executed against a Storage
  component and sets the duration based on `tx.started` and `tx.finished`
  """
  @spec set_finished(%Transaction{}, non_neg_integer(), atom()) :: %Transaction{}
  def set_finished(tx, time, physical_node) do
    # duration in microseconds
    duration_mus = DateTime.diff(time, tx.started, :microsecond)
    # duration in milliseconds
    duration_ms = duration_mus / 1000

    Debug.Timing.log("{#{tx.id}} executed in #{duration_ms}ms on #{physical_node}")

    %{tx |
      finished: time, 
      duration: duration_ms
    }
  end

  @doc """
  Adds a timestamp to the Transaction based on local system clock
  """
  @spec add_timestamp(%Transaction{}) :: %Transaction{}
  def add_timestamp(tx) do
    %{tx | timestamp: DateTime.utc_now()}
  end

  @doc """
  Sets the participating partitions for this Transaction to the given set of partitions
  """
  @spec set_participating_partitions(%Transaction{}, %MapSet{}) :: %Transaction{}
  def set_participating_partitions(tx, partitions) do
    %{tx | participating_partitions: partitions}
  end


  @doc """
  Given a list of `Transaction.Op` Transaction operations, generates
  a read set for the operations, which is a set of values that the
  operations perform any type of read on
  """
  @spec generate_read_set([%Transaction.Op{}]) :: %MapSet{}
  def generate_read_set(operations) do
    # reduce all of the operations into a set by 
    # adding to set if the operation is a READ
    Enum.reduce(operations, MapSet.new(), 
      fn op, acc ->
        case op.type do
          :READ ->
            MapSet.put(acc, op.key)
          _ -> 
            acc
        end
      end
    )
  end

  @doc """
  Given a list of `Transaction.Op` Transaction operations, generates
  a write set for the operations, which is a set of values that the
  operations perform any type of write on
  """
  @spec generate_write_set([%Transaction.Op{}]) :: %MapSet{}
  def generate_write_set(operations) do
    # reduce all of the operations into a set by adding to set
    # if the operation is a CREATE, UPDATE, or DELETE
    Enum.reduce(operations, MapSet.new(), 
      fn op, acc ->
        case op.type do
          :CREATE ->
            MapSet.put(acc, op.key)
          :UPDATE ->
            MapSet.put(acc, op.key)
          :DELETE ->
            MapSet.put(acc, op.key)
          _ -> 
            acc
        end
      end
    )
  end

  @doc """
  Creates a new Transaction with given operations and a given id for
  debugging purposes
  """
  @spec new([%Transaction.Op{}], atom()) :: %Transaction{}
  def new(operations, id) do
    tx = Transaction.new(operations)
    %{tx | id: id}
  end

  @doc """
  Creates a new Transaction with given operations and generates the read
  and write sets for the Transaction based on the Operations that make up
  the Transaction
  """
  @spec new([%Transaction.Op{}]) :: %Transaction{}
  def new(operations) do
    %Transaction{
      operations: operations,
      read_set: Transaction.generate_read_set(operations),
      write_set: Transaction.generate_write_set(operations)
    }
  end
end