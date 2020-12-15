defmodule PartitioningTest do
  use ExUnit.Case

  doctest Configuration
  doctest PartitionScheme
  doctest ReplicationScheme.Async

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "PartitionScheme partition view works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=3)
    )
    partitions = PartitionScheme.get_partition_view(configuration.partition_scheme)
    
    assert length(partitions) == 3, "Expected the partition view to have 3 partitions"
    assert partitions == [1, 2, 3]
  end

  test "PartitionScheme get_all_other_partitions/2 works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
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

  test "PartitionScheme generate_partition_key_map/1 works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=1)
    )
    partition_map = configuration.partition_scheme.partition_key_map

    # expecting all keys in partition map to map to partition 1 since
    # the Configuration is created with a single partition

    assert Map.get(partition_map, :a) == 1
    assert Map.get(partition_map, :z) == 1

    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=4)
    )
    partition_map = configuration.partition_scheme.partition_key_map

    # expecting the partition map to partition the key range
    # into 4 chunks of [a-g] -> 1, [h-n] -> 2, [o-u] -> 3, [v-z] -> 4

    assert Map.get(partition_map, :a) == 1
    assert Map.get(partition_map, :h) == 2
    assert Map.get(partition_map, :o) == 3
    assert Map.get(partition_map, :z) == 4
  end

  test "PartitionScheme generate_participating_partitions/2 works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=3)
    )
    partition_scheme = configuration.partition_scheme

    # create a couple of Transactions
    tx1 = Transaction.new(_operations=[
      Transaction.Op.read(:z),
      Transaction.Op.create(:a, Transaction.Expression.new(:z, :+, 1))
    ])
    tx2 = Transaction.new(_operations=[Transaction.Op.create(:a, 1)])
    tx3 = Transaction.new(_operations=[Transaction.Op.create(:n, 1)])
    tx_batch = [tx1, tx2, tx3]

    tx_batch = PartitionScheme.generate_participating_partitions(
      _tx_batch=tx_batch, partition_scheme
    )

    tx1 = Enum.at(tx_batch, 0)
    tx2 = Enum.at(tx_batch, 1)
    tx3 = Enum.at(tx_batch, 2)

    # check active participating partitions
    tx1_active = tx1.active_participants
    tx2_active = tx2.active_participants
    tx3_active = tx3.active_participants

    assert MapSet.to_list(tx1_active) == [1]
    assert MapSet.to_list(tx2_active) == [1]
    assert MapSet.to_list(tx3_active) == [2]

    # check passive participating partitions
    tx1_passive = tx1.passive_participants
    tx2_passive = tx2.passive_participants
    tx3_passive = tx3.passive_participants

    assert MapSet.to_list(tx1_passive) == [3]
    assert MapSet.to_list(tx2_passive) == []
    assert MapSet.to_list(tx3_passive) == []

    # check all participating partitions
    tx1_all_partitions = tx1.participating_partitions
    tx2_all_partitions = tx2.participating_partitions
    tx3_all_partitions = tx3.participating_partitions

    # tx1 has participating partitions 1, 3, since `a` is in key range for
    # partition 1 and `z` is in key range for partition 3
    assert MapSet.size(tx1_all_partitions) == 2
    assert MapSet.member?(tx1_all_partitions, 1) == true
    assert MapSet.member?(tx1_all_partitions, 3) == true

    # tx2 only access `a`, thus only participating partition is partition 1
    assert MapSet.size(tx2_all_partitions) == 1
    assert MapSet.member?(tx2_all_partitions, 1) == true

    # tx3 only access `n`, thus only participating partition is partition 2
    assert MapSet.size(tx3_all_partitions) == 1
    assert MapSet.member?(tx3_all_partitions, 2) == true
  end

  test "PartitionScheme partition_transactions/1 works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=3)
    )
    partition_scheme = configuration.partition_scheme

    # create a couple of Transactions
    tx1 = Transaction.new(_operations=[Transaction.Op.create(:a, 1)])
    tx2 = Transaction.new(_operations=[Transaction.Op.create(:m, 1)])
    tx3 = Transaction.new(_operations=[Transaction.Op.create(:z, 1)])
    tx_batch = [tx1, tx2, tx3]

    # generate the participating partitions for every Transaction and partition the batch
    tx_batch = PartitionScheme.generate_participating_partitions(
      _tx_batch=tx_batch, partition_scheme
    )
    partitioned_batch = PartitionScheme.partition_transactions(_tx_batch=tx_batch)

    # check that each partitioned batch contains a single Transaction
    # for that partition
    partition_1_batch = Map.get(partitioned_batch, 1)
    partition_2_batch = Map.get(partitioned_batch, 2)
    partition_3_batch = Map.get(partitioned_batch, 3)

    assert length(partition_1_batch) == 1
    assert length(partition_2_batch) == 1
    assert length(partition_3_batch) == 1

    # check that each partitioned batch contains the correct
    # Transaction for that partition
    assert Enum.at(partition_1_batch, 0).operations |> Enum.at(0) |> Map.get(:key) == :a
    assert Enum.at(partition_2_batch, 0).operations |> Enum.at(0) |> Map.get(:key) == :m
    assert Enum.at(partition_3_batch, 0).operations |> Enum.at(0) |> Map.get(:key) == :z
  end

  test "PartitionScheme is_local?/3 works as expected" do
    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=3)
    )
    partition_scheme = configuration.partition_scheme

    assert PartitionScheme.is_local?(:a, _partition=1, _partition_scheme=partition_scheme) == true
    assert PartitionScheme.is_local?(:Alice, _partition=1, _partition_scheme=partition_scheme) == true

    assert PartitionScheme.is_local?(:z, _partition=1, _partition_scheme=partition_scheme) == false
  end
end
