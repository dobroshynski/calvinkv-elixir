defmodule StorageTest.Raft do
  use ExUnit.Case

  doctest Calvin
  doctest PartitionScheme
  doctest ReplicationScheme.Raft

  import Emulation, only: [spawn: 2]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3]
  
  test "Raft replication works as expected" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration using Raft for replication
    num_partitions = 2
    configuration = Configuration.new(
      _replication=ReplicationScheme.Raft.new(_num_replicas=3, _num_partitions=num_partitions), 
      _partition=PartitionScheme.new(_num_partitions=num_partitions)
    )

    # launch the Calvin components
    Calvin.launch(configuration)

    spawn(
      :client,
      fn ->
        # connect to the Sequencer on the leader replica
        sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # send a Transaction with a couple of operations to the Sequencer
        tx = Transaction.new(_operations=[
          Transaction.Op.create(:a, 1),
          Transaction.Op.create(:z, 1)
        ])
        Client.send_tx(client, tx)
        
        # wait for this epoch to finish and for Raft replication to finish
        :timer.sleep(3000)

        # get the key-value stores from every Storage component on
        # all replicas and check that they contain the expected data,
        # with partition 1 range [a-m] and partition 2 range [n-z]

        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :A)
        )

        kv_store_part_1 = Enum.at(kv_stores, 0)
        assert Map.get(kv_store_part_1, :a) == 1
        assert Map.get(kv_store_part_1, :z) == nil

        kv_store_part_2 = Enum.at(kv_stores, 1)
        assert Map.get(kv_store_part_2, :a) == nil
        assert Map.get(kv_store_part_2, :z) == 1

        # perform the same check on replica B
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :B)
        )

        kv_store_part_1 = Enum.at(kv_stores, 0)
        assert Map.get(kv_store_part_1, :a) == 1
        assert Map.get(kv_store_part_1, :z) == nil

        kv_store_part_2 = Enum.at(kv_stores, 1)
        assert Map.get(kv_store_part_2, :a) == nil
        assert Map.get(kv_store_part_2, :z) == 1

        # perform the same check on replica C
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :C)
        )

        kv_store_part_1 = Enum.at(kv_stores, 0)
        assert Map.get(kv_store_part_1, :a) == 1
        assert Map.get(kv_store_part_1, :z) == nil

        kv_store_part_2 = Enum.at(kv_stores, 1)
        assert Map.get(kv_store_part_2, :a) == nil
        assert Map.get(kv_store_part_2, :z) == 1
      end
    )
    
    # timeout after a couple epochs
    wait_timeout = 5000

    receive do
    after
      wait_timeout -> :ok
    end
  after
    Emulation.terminate()
  end

  test "Raft replication with multiple clients on different partitions works as expected" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration with 7 replicas partitioned across 3 nodes 
    # using Raft for replication
    num_partitions = 3
    configuration = Configuration.new(
      _replication=ReplicationScheme.Raft.new(_num_replicas=7, _num_partitions=num_partitions), 
      _partition=PartitionScheme.new(_num_partitions=num_partitions)
    )

    # launch the Calvin components
    Calvin.launch(configuration)

    spawn(
      :client,
      fn ->
        # connect to the Sequencer on partition 1
        sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # send a Transaction with a couple of operations
        tx = Transaction.new(_operations=[
          Transaction.Op.create(:a, 1),
          Transaction.Op.create(:z, 1)
        ])
        Client.send_tx(client, tx)

        # connect to the Sequencer on partition 2
        sequencer = Component.id(_replica=:A, _partition=2, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # send a Transaction with a couple of operations
        tx = Transaction.new(_operations=[
          Transaction.Op.create(:b, 1),
          Transaction.Op.create(:w, 1)
        ])
        Client.send_tx(client, tx)
        
        # wait for this epoch to finish and for Raft replication to finish
        :timer.sleep(5000)

        # get the key-value stores from every Storage component on
        # replica A and check that they contain the expected data

        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :A)
        )
        
        kv_store_part_1 = Enum.at(kv_stores, 0)
        kv_store_part_2 = Enum.at(kv_stores, 1)
        kv_store_part_3 = Enum.at(kv_stores, 2)

        assert kv_store_part_1 == %{a: 1, b: 1}
        assert kv_store_part_2 == %{}
        assert kv_store_part_3 == %{w: 1, z: 1}

        # perform the same checks on replica G
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :G)
        )
        
        kv_store_part_1 = Enum.at(kv_stores, 0)
        kv_store_part_2 = Enum.at(kv_stores, 1)
        kv_store_part_3 = Enum.at(kv_stores, 2)

        assert kv_store_part_1 == %{a: 1, b: 1}
        assert kv_store_part_2 == %{}
        assert kv_store_part_3 == %{w: 1, z: 1}
      end
    )
    
    # timeout after a couple epochs
    wait_timeout = 10000

    receive do
    after
      wait_timeout -> :ok
    end
  after
    Emulation.terminate()
  end

  test "Raft replication with multiple clients on different replicas works as expected" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration with 3 replicas partitioned across 3 nodes 
    # using Raft for replication
    num_partitions = 3
    configuration = Configuration.new(
      _replication=ReplicationScheme.Raft.new(_num_replicas=3, _num_partitions=num_partitions), 
      _partition=PartitionScheme.new(_num_partitions=num_partitions)
    )

    # launch the Calvin components
    Calvin.launch(configuration)

    spawn(
      :client,
      fn ->
        # connect to the Sequencer on partition 2 of replica A
        sequencer = Component.id(_replica=:A, _partition=2, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # send a Transaction with a couple of operations
        tx = Transaction.new(_operations=[
          Transaction.Op.create(:a, 1),
          Transaction.Op.create(:m, 1),
          Transaction.Op.create(:z, 1)
        ])
        Client.send_tx(client, tx)

        # connect to the Sequencer on partition 1 of replica C
        sequencer = Component.id(_replica=:C, _partition=1, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # send a Transaction with a couple of operations
        tx = Transaction.new(_operations=[
          Transaction.Op.create(:b, 1),
          Transaction.Op.create(:w, 1)
        ])
        Client.send_tx(client, tx)
        
        # wait for this epoch to finish and for Raft replication to finish
        :timer.sleep(3000)

        # get the key-value stores from every Storage component on
        # replica A and check that they contain the expected data

        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :A)
        )
        
        kv_store_part_1 = Enum.at(kv_stores, 0)
        kv_store_part_2 = Enum.at(kv_stores, 1)
        kv_store_part_3 = Enum.at(kv_stores, 2)

        assert kv_store_part_1 == %{a: 1, b: 1}
        assert kv_store_part_2 == %{m: 1}
        assert kv_store_part_3 == %{w: 1, z: 1}

        # perform the same checks on replica B
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :B)
        )

        kv_store_part_1 = Enum.at(kv_stores, 0)
        kv_store_part_2 = Enum.at(kv_stores, 1)
        kv_store_part_3 = Enum.at(kv_stores, 2)

        assert kv_store_part_1 == %{a: 1, b: 1}
        assert kv_store_part_2 == %{m: 1}
        assert kv_store_part_3 == %{w: 1, z: 1}

        # perform the same checks on replica C
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :C)
        )

        kv_store_part_1 = Enum.at(kv_stores, 0)
        kv_store_part_2 = Enum.at(kv_stores, 1)
        kv_store_part_3 = Enum.at(kv_stores, 2)

        assert kv_store_part_1 == %{a: 1, b: 1}
        assert kv_store_part_2 == %{m: 1}
        assert kv_store_part_3 == %{w: 1, z: 1}
      end
    )
    
    # timeout after a couple epochs
    wait_timeout = 5000

    receive do
    after
      wait_timeout -> :ok
    end
  after
    Emulation.terminate()
  end
end
