defmodule ReplicationTest do
  use ExUnit.Case

  doctest Configuration
  doctest PartitionScheme
  doctest AsyncReplicationScheme

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "AsyncReplicationScheme main replica works as expected" do
    # create a configuration without setting a main replica
    # which should default to the first replica A
    configuration = Configuration.new(
      _replication=AsyncReplicationScheme.new(_num_replicas=3), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )
    assert configuration.replication_scheme.main_replica == :A

    configuration = Configuration.new(
      _replication=AsyncReplicationScheme.new(_num_replicas=3, _main_replica=:B), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )
    assert configuration.replication_scheme.main_replica == :B
  end

  test "ReplicationScheme replica view works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=AsyncReplicationScheme.new(_num_replicas=3), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )

    replicas = ReplicationScheme.get_replica_view(configuration.replication_scheme)
    
    assert length(replicas) == 3, "Expected the replica view to have 3 replicas"
    assert replicas == [:A, :B, :C]
  end

  test "ReplicationScheme get_all_other_replicas() works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=AsyncReplicationScheme.new(_num_replicas=4), 
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
end
