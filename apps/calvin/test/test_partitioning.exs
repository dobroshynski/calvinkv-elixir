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
end
