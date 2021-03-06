defmodule ComponentTest do
  use ExUnit.Case
  doctest Component
  doctest Configuration

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Component id/1 works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=1)
    )

    # create a Sequencer process
    sequencer = Sequencer.new(_replica=:A, _partition=1, configuration)
    sequencer_proc_id = Component.id(sequencer)

    assert to_charlist(sequencer_proc_id) == 'A1-sequencer'
  end

  test "Component id/3 works as expected" do
    # generate some unique ids
    sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
    scheduler = Component.id(_replica=:B, _partition=2, _type=:scheduler)
    storage = Component.id(_replica=:C, _partition=3, _type=:storage)

    assert to_charlist(sequencer) == 'A1-sequencer'
    assert to_charlist(scheduler) == 'B2-scheduler'
    assert to_charlist(storage) == 'C3-storage'
  end

  test "Component `physical` node id generation works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=1)
    )

    # create a Sequencer process
    sequencer = Sequencer.new(_replica=:A, _partition=1, configuration)
    # get the `physical` node id
    id = Component.physical_node_id(sequencer)

    assert to_charlist(id) == 'A1'
  end

  test "Component on_main_replica?/1 for ReplicationScheme.Async works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=3), 
      _partition=PartitionScheme.new(_num_partitions=1)
    )

    # create a Sequencer on replica A
    sequencer = Sequencer.new(_replica=:A, _partition=1, configuration)
    assert Component.on_main_replica?(sequencer) == true

    # create a Sequencer on replica B
    sequencer = Sequencer.new(_replica=:B, _partition=1, configuration)
    assert Component.on_main_replica?(sequencer) == false
  end

  test "Component on_leader_replica?/1 for ReplicationScheme.Raft works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Raft.new(_num_replicas=3, _num_partitions=1), 
      _partition=PartitionScheme.new(_num_partitions=1)
    )

    # create a Sequencer on replica A
    sequencer = Sequencer.new(_replica=:A, _partition=1, configuration)
    assert Component.on_leader_replica?(sequencer) == true

    # create a Sequencer on replica B
    sequencer = Sequencer.new(_replica=:B, _partition=1, configuration)
    assert Component.on_leader_replica?(sequencer) == false
  end
end
  