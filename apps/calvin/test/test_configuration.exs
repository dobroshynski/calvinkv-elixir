defmodule ConfigurationTest do
  use ExUnit.Case
  doctest Configuration

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Configuration partition view works as expected" do
    # create a configuration
    configuration = Configuration.new(_num_replicas=2, _num_partitions=3)

    partitions = Configuration.get_partition_view(configuration)
    
    assert length(partitions) == 3, "Expected the partition view to have 3 partitions"
    assert partitions == [1, 2, 3]
  end

  test "Configuration replica view works as expected" do
    # create a configuration
    configuration = Configuration.new(_num_replicas=3, _num_partitions=2)

    replicas = Configuration.get_replica_view(configuration)
    
    assert length(replicas) == 3, "Expected the replica view to have 3 replicas"
    assert replicas == [:A, :B, :C]
  end

  test "Configuration get_all_other_partitions() works as expected" do
    # create a configuration
    configuration = Configuration.new(_num_replicas=1, _num_partitions=3)

    # create Sequencers
    sequencer_proc_part_1 = Sequencer.new(_replica=:A, _partition=1, configuration)
    sequencer_proc_part_2 = Sequencer.new(_replica=:A, _partition=2, configuration)

    # get partitions other than the partition assigned to the given process
    other_partitions = Configuration.get_all_other_partitions(sequencer_proc_part_1, configuration)
    
    assert length(other_partitions) == 2, "Expected the number of other partitions to be 2"
    assert other_partitions == [2, 3]

    other_partitions = Configuration.get_all_other_partitions(sequencer_proc_part_2, configuration)
    
    assert length(other_partitions) == 2, "Expected the number of other partitions to be 2"
    assert other_partitions == [1, 3]
  end

  test "Configuration get_all_other_replicas() works as expected" do
    # create a configuration
    configuration = Configuration.new(_num_replicas=4, _num_partitions=2)

    # create Sequencers
    sequencer_proc_repl_a = Sequencer.new(_replica=:A, _partition=1, configuration)
    sequencer_proc_repl_b = Sequencer.new(_replica=:B, _partition=1, configuration)

    # get replicas other than the replica assigned to the given process
    other_replicas = Configuration.get_all_other_replicas(sequencer_proc_repl_a, configuration)
    
    assert length(other_replicas) == 3, "Expected the number of other replicas to be 3"
    assert other_replicas == [:B, :C, :D]

    other_replicas = Configuration.get_all_other_replicas(sequencer_proc_repl_b, configuration)
    
    assert length(other_replicas) == 3, "Expected the number of other replicas to be 3"
    assert other_replicas == [:A, :C, :D]
  end
end
