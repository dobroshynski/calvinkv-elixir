defmodule ReplicationTest do
  use ExUnit.Case

  doctest Configuration
  doctest PartitionScheme
  doctest ReplicationScheme.Async

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "ReplicationScheme.Async main replica works as expected" do
    # create a configuration without setting a main replica
    # which should default to the first replica A
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=3), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )
    assert configuration.replication_scheme.main_replica == :A

    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=3, _main_replica=:B), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )
    assert configuration.replication_scheme.main_replica == :B
  end

  test "ReplicationScheme replica view works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=3), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )

    replicas = ReplicationScheme.get_replica_view(configuration.replication_scheme)
    
    assert length(replicas) == 3, "Expected the replica view to have 3 replicas"
    assert replicas == [:A, :B, :C]
  end

  test "ReplicationScheme get_all_other_replicas() works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=4), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )

    # create Sequencers
    sequencer_proc_repl_a = Sequencer.new(_replica=:A, _partition=1, configuration)
    sequencer_proc_repl_b = Sequencer.new(_replica=:B, _partition=1, configuration)

    # get replicas other than the replica assigned to the given process
    other_replicas = ReplicationScheme.get_all_other_replicas(sequencer_proc_repl_a, configuration.replication_scheme)
    
    assert length(other_replicas) == 3, "Expected the number of other replicas to be 3"
    assert other_replicas == [:B, :C, :D]

    other_replicas = ReplicationScheme.get_all_other_replicas(sequencer_proc_repl_b, configuration.replication_scheme)
    
    assert length(other_replicas) == 3, "Expected the number of other replicas to be 3"
    assert other_replicas == [:A, :C, :D]
  end

  test "ReplicationScheme.Raft initial leaders are initiallized as expected" do
    # create a configuration using Raft for replication
    num_partitions = 2
    configuration = Configuration.new(
      _replication=ReplicationScheme.Raft.new(_num_replicas=3, _num_partitions=num_partitions), 
      _partition=PartitionScheme.new(_num_partitions=num_partitions)
    )
    current_leaders = configuration.replication_scheme.current_leaders

    # make sure that the initial leader for each partition's replication group is
    # set correctly
    Enum.map(PartitionScheme.get_partition_view(configuration.partition_scheme),
      fn partition -> 
        assert Map.get(current_leaders, partition) == :A
      end
    )
  end

  test "ReplicationScheme.Raft.Log and Log.Entry work as expected" do
    # create a new Raft log
    log = ReplicationScheme.Raft.Log.new()
    assert log.entries == []

    # create an empty log entry
    entry = ReplicationScheme.Raft.Log.Entry.empty()
    assert entry.index == 0 && entry.term == 0 && entry.batch == []

    # create a log entry with some Transactions
    entry = ReplicationScheme.Raft.Log.Entry.new(_index=1, _term=1, _batch=[
        Transaction.new(_operations=[
          Transaction.Op.create(:a, 1),
          Transaction.Op.create(:b, 1)    
        ]),
        Transaction.new(_operations=[
          Transaction.Op.update(:a, 0)
        ])
      ]
    )
    assert entry.index == 1 && entry.term == 1 && length(entry.batch) == 2
  end

  test "ReplicationScheme.Raft.Log utility functions work as expected" do
    # test with an empty log
    log = ReplicationScheme.Raft.Log.new()

    assert ReplicationScheme.Raft.Log.get_last_log_index(log) == 0
    assert ReplicationScheme.Raft.Log.get_last_log_term(log) == 0
    assert ReplicationScheme.Raft.Log.logged?(log, 1) == false

    assert ReplicationScheme.Raft.Log.get_entry_at_index(log, 1) == :noentry
    assert ReplicationScheme.Raft.Log.get_entries_at_index(log, 1) == []
    assert ReplicationScheme.Raft.Log.remove_entries(log, _from_index=1).entries == []

    # add some log entries to the log and test the utility functions
    log = ReplicationScheme.Raft.Log.add_entries(log, _entries=[
      ReplicationScheme.Raft.Log.Entry.new(_index=1, _term=1, _batch=[
        Transaction.new(_operations=[
          Transaction.Op.create(:a, 1)
        ]),
      ]), 
      ReplicationScheme.Raft.Log.Entry.new(_index=2, _term=1, _batch=[
        Transaction.new(_operations=[
          Transaction.Op.create(:b, 1)
        ])
      ])
    ])
    assert length(log.entries) == 2

    assert ReplicationScheme.Raft.Log.get_last_log_index(log) == 2
    assert ReplicationScheme.Raft.Log.get_last_log_term(log) == 1
    assert ReplicationScheme.Raft.Log.logged?(log, 2) == true
    assert ReplicationScheme.Raft.Log.logged?(log, 3) == false

    assert length(ReplicationScheme.Raft.Log.get_entry_at_index(log, 1).batch) == 1
    assert length(ReplicationScheme.Raft.Log.get_entries_at_index(log, 1)) == 2
    assert length(ReplicationScheme.Raft.Log.get_entries_at_index(log, 2)) == 1
    assert ReplicationScheme.Raft.Log.get_entry_at_index(log, 3) == :noentry

    assert length(ReplicationScheme.Raft.Log.remove_entries(log, _from_index=2).entries) == 1
  end
end
