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

  # Module for a Raft log comprising of Log.Entry entries. The log is 1-index based, and the
  # index in the Log corresponds to the epoch of the batch stored in the Entry at that index

  # For example, given Transactions [tx1, tx2] batched during epoch 1 and Transactions
  # [tx3, tx4] batched during epoch 2, the Log contains 2 entries in `entries`
  # with epoch 1 batch at index 1 and epoch 2 batch at index 2

  defmodule Log do
    @enforce_keys [:entries]

    defstruct(
      entries: nil
    )

    @doc """
    Creates a new empty Raft log
    """
    @spec new() :: %Log{}
    def new() do
      %Log{
        entries: []
      }
    end

    @doc """
    Given a Log and a 1-based index (likely corresponding to an epoch), returns the Log.Entry 
    entry at that index
    """
    @spec get_entry_at_index(%Log{}, non_neg_integer()) :: :no_entry | %Log.Entry{}
    def get_entry_at_index(log, idx) do
      if idx <= 0 || length(log.entries) < idx do
        :noentry
      else
        # since the indexing is meant to be 1-based in order to
        # match up to epoch numbers, offset the given index by 1
        Enum.at(log.entries, idx - 1)
      end
    end

    @doc """
    Given a Log and a 1-based index (likely corresponding to an epoch), returns a suffix of
    Log.Entry entries starting at the given index
    """
    @spec get_entries_at_index(%Log{}, non_neg_integer()) :: [%Log.Entry{}]
    def get_entries_at_index(log, idx) do
      if idx > length(log.entries) do
        []
      else
        Enum.slice(log.entries, idx - 1, length(log.entries))
      end
    end

    @doc """
    Given a Log, returns the index for the last log entry
    """
    @spec get_last_log_index(%Log{}) :: non_neg_integer()
    def get_last_log_index(log) do
      Enum.at(log.entries, length(log.entries) - 1, Log.Entry.empty()).index
    end

    @doc """
    Given a Log, returns the Raft term for the last log entry
    """
    @spec get_last_log_term(%Log{}) :: non_neg_integer()
    def get_last_log_term(log) do
      Enum.at(log.entries, length(log.entries) - 1, Log.Entry.empty()).term
    end

    @doc """
    Given a Log and a 1-based index (likely corresponding to an epoch), removes all 
    log entries at the given index and larger, and returns the updated Log
    """
    @spec remove_entries(%Log{}, non_neg_integer()) :: %Log{}
    def remove_entries(log, idx) do
      if idx > length(log.entries) do
        log
      else
        # compute how many entries to remove and multiply by -1
        # since need to remove from the back of the log
        num_drop = length(log.entries) - idx + 1
        %{log | entries: Enum.drop(log.entries, -num_drop)}
      end
    end

    @doc """
    Given a Log and a list of `Log.Entry` entries, appends the entries to the
    Log and returns the updated Log
    """
    @spec add_entries(%Log{}, [%Log.Entry{}]) :: %Log{}
    def add_entries(log, entries) do
      %{log | entries: log.entries ++ entries}
    end

    @doc """
    Given a Log and a 1-based index, returns whether the log entry at the index
    exists
    """
    @spec logged?(%Log{}, non_neg_integer()) :: boolean()
    def logged?(log, idx) do
      if idx > 0 && idx <= length(log.entries) do
        true
      else
        false
      end
    end

    # Module for a single Raft log entry in the system. Each individual entry is a batch
    # of Transactions for a certain epoch

    defmodule Entry do
      @enforce_keys [:index, :term, :batch]

      defstruct(
        index: nil,
        term: nil,
        batch: nil
      )

      @doc """
      Creates a new empty log Entry
      """
      @spec empty() :: %Log.Entry{}
      def empty() do
        %Log.Entry{index: 0, term: 0, batch: []}
      end

      @doc """
      Creates a new log Entry with a given batch of Transactions, for a given
      index in the Log, and during a given Raft term
      """
      @spec new(non_neg_integer(), non_neg_integer(), [%Transaction{}]) :: %Log.Entry{}
      def new(index, term, tx_batch) do
        %Log.Entry{
          index: index,
          term: term,
          batch: tx_batch
        }
      end
    end
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