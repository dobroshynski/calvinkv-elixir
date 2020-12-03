defmodule ConfigurationTest do
  use ExUnit.Case
  doctest Configuration

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Configuration partition view works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=2), 
      _partition=PartitionScheme.new(_num_partitions=3)
    )

    partitions = Configuration.get_partition_view(configuration)
    
    assert length(partitions) == 3, "Expected the partition view to have 3 partitions"
    assert partitions == [1, 2, 3]
  end

  test "Configuration replica view works as expected" do
    # create a configuration using async replication
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=3), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )
    replicas = Configuration.get_replica_view(configuration)

    assert length(replicas) == 3, "Expected the replica view to have 3 replicas"
    assert replicas == [:A, :B, :C]

    # create a configuration using Raft replication
    configuration = Configuration.new(
      _replication=ReplicationScheme.Raft.new(_num_replicas=3, _num_partitions=2), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )
    replicas = Configuration.get_replica_view(configuration)
    
    assert length(replicas) == 3, "Expected the replica view to have 3 replicas"
    assert replicas == [:A, :B, :C]
  end

  test "Configuration using_replication?/1 works as expected" do
    # create a configuration using async replication
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=3), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )
    assert Configuration.using_replication?(configuration) == :async

    # create a configuration using Raft replication
    configuration = Configuration.new(
      _replication=ReplicationScheme.Raft.new(_num_replicas=3, _num_partitions=2), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )
    assert Configuration.using_replication?(configuration) == :raft
  end
end
