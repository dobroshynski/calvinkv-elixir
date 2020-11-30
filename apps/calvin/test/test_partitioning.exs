defmodule PartitioningTest do
  use ExUnit.Case

  doctest Configuration
  doctest PartitionScheme
  doctest AsyncReplicationScheme

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "PartitionScheme partition view works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=AsyncReplicationScheme.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=3)
    )
    partitions = PartitionScheme.get_partition_view(configuration.partition_scheme)
    
    assert length(partitions) == 3, "Expected the partition view to have 3 partitions"
    assert partitions == [1, 2, 3]
  end

  test "PartitionScheme get_all_other_partitions() works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=AsyncReplicationScheme.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=3)
    )

    # create Sequencers
    sequencer_proc_part_1 = Sequencer.new(_replica=:A, _partition=1, configuration)
    sequencer_proc_part_2 = Sequencer.new(_replica=:A, _partition=2, configuration)

    # get partitions other than the partition assigned to the given process
    other_partitions = PartitionScheme.get_all_other_partitions(sequencer_proc_part_1, configuration.partition_scheme)
    
    assert length(other_partitions) == 2, "Expected the number of other partitions to be 2"
    assert other_partitions == [2, 3]

    other_partitions = PartitionScheme.get_all_other_partitions(sequencer_proc_part_2, configuration.partition_scheme)
    
    assert length(other_partitions) == 2, "Expected the number of other partitions to be 2"
    assert other_partitions == [1, 3]
  end

  test "PartitionScheme generate_key_partition_map() works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=AsyncReplicationScheme.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=1)
    )
    partition_map = configuration.partition_scheme.key_partition_map

    # expecting all keys in partition map to map to partition 1 since
    # the Configuration is created with a single partition

    assert Map.get(partition_map, :a) == 1
    assert Map.get(partition_map, :z) == 1

    # create a configuration
    configuration = Configuration.new(
      _replication=AsyncReplicationScheme.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=4)
    )
    partition_map = configuration.partition_scheme.key_partition_map

    # expecting the partition map to partition the key range
    # into 4 chunks of [a-g] -> 1, [h-n] -> 2, [o-u] -> 3, [v-z] -> 4

    assert Map.get(partition_map, :a) == 1
    assert Map.get(partition_map, :h) == 2
    assert Map.get(partition_map, :o) == 3
    assert Map.get(partition_map, :z) == 4
  end
end
