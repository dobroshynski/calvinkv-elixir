defmodule StorageTest.Async do
  use ExUnit.Case

  doctest Calvin
  doctest PartitionScheme
  doctest ReplicationScheme.Async

  import Emulation, only: [spawn: 2]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3]
  
  test "Commands are executed against Storage components by all partitions" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(0)])

    # create a configuration
    # single replica partitioned across 3 nodes
    replica = :A
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=3)
    )

    # launch the Calvin components
    Calvin.launch(configuration)
    
    client =
      spawn(
        :client,
        fn ->
          # connect to the Sequencer on partition 1
          default_sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
          client = Client.connect_to(default_sequencer)

          # send a couple of Transaction requests to the Sequencer
          Client.send_create_tx(client, :a, 1)
          Client.send_create_tx(client, :b, 2)
          Client.send_create_tx(client, :z, 1)

          Client.send_create_tx(client, :d, 1)
          Client.send_update_tx(client, :d, 0)
          Client.send_delete_tx(client, :d)
          
          # wait for this epoch to finish, then send some more requests
          :timer.sleep(3000)

          # get the key-value stores from every Storage component
          kv_stores = Testing.get_kv_stores(_ids=Configuration.get_storage_view(configuration, replica))

          # check that every Storage node has the expected key-value store
          # based on the current PartitionScheme with 3 partitions

          # partition 1 Storage should have the `a` and `b` records
          kv_store = Enum.at(kv_stores, 0)
          assert Map.get(kv_store, :a) == 1
          assert Map.get(kv_store, :b) == 2

          # partition 2 Storage shouldn't have any data
          kv_store = Enum.at(kv_stores, 1)
          assert kv_store == %{}

          # partition 2 Storage should have the `z` record
          kv_store = Enum.at(kv_stores, 2)
          assert Map.get(kv_store, :z) == 1

          Client.send_create_tx(client, :c, 3)
        end
      )

    # timeout after a couple epochs
    wait_timeout = 5000

    receive do
    after
      wait_timeout -> :ok
    end

    handle = Process.monitor(client)
    # timeout
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      30_000 -> false
    end
  after
    Emulation.terminate()
  end

  test "Commands from multiple clients are executed against Storage components" do
    Emulation.init()

    # create a configuration
    # single replica partitioned across 3 nodes
    replica = :A
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=3)
    )
    
    # launch the Calvin components
    Calvin.launch(configuration)
      
    # first client connects to Sequencer on partition 1
    spawn(
      :client_1,
      fn ->
        # connect to the Sequencer on partition 1
        partition1_sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
        client = Client.connect_to(partition1_sequencer)

        # send a couple of Transaction requests to the Sequencer
        Client.send_create_tx(client, :a, 1)
        Client.send_create_tx(client, :b, 2)
        
        # wait for this epoch to finish and for batches from other Sequencer to arrive
        :timer.sleep(3000)

        # get the key-value stores from every Storage component
        kv_stores = Testing.get_kv_stores(_ids=Configuration.get_storage_view(configuration, replica))

        # check that every Storage node has the expected key-value store
        # based on the current PartitionScheme with 3 partitions

        # partition 1 Storage should have the `a`, `b`, `c` records
        kv_store_part_1 = Enum.at(kv_stores, 0)
        assert Map.get(kv_store_part_1, :a) == 1
        assert Map.get(kv_store_part_1, :b) == 1
        assert Map.get(kv_store_part_1, :c) == 1

        # partition 2, 3 Storage shouldn't have any data
        kv_store_part_2 = Enum.at(kv_stores, 1)
        kv_store_part_3 = Enum.at(kv_stores, 2)

        assert kv_store_part_2 == %{}
        assert kv_store_part_3 == %{}
      end
    )

    # second client connects to Sequencer on partition 2
    spawn(
      :client_2,
      fn ->
        # connect to the Sequencer on partition 2
        partition2_sequencer = Component.id(_replica=:A, _partition=2, _type=:sequencer)
        client = Client.connect_to(partition2_sequencer)

        # send a couple of Transaction requests to the Sequencer
        Client.send_create_tx(client, :c, 1)
        # wait a bit to make sure that the update from client 2 comes 
        # after the create tx from client 1
        :timer.sleep(1000)
        Client.send_update_tx(client, :b, 1)
        
        # wait for this epoch to finish and for batches from other Sequencer to arrive
        :timer.sleep(3000)

        # get the key-value stores from every Storage component
        kv_stores = Testing.get_kv_stores(_ids=Configuration.get_storage_view(configuration, replica))

        # check that every Storage node has the expected key-value store
        # based on the current PartitionScheme with 3 partitions

        # partition 1 Storage should have the `a`, `b`, `c` records
        kv_store_part_1 = Enum.at(kv_stores, 0)
        assert Map.get(kv_store_part_1, :a) == 1
        assert Map.get(kv_store_part_1, :b) == 1
        assert Map.get(kv_store_part_1, :c) == 1

        # partition 2, 3 Storage shouldn't have any data
        kv_store_part_2 = Enum.at(kv_stores, 1)
        kv_store_part_3 = Enum.at(kv_stores, 2)

        assert kv_store_part_2 == %{}
        assert kv_store_part_3 == %{}
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

  test "Requests to non-main replicas get forwarded to main replica" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration with 2 replicas
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=2, _main_replica=:A), 
      _partition=PartitionScheme.new(_num_partitions=3)
    )
    
    # launch the Calvin components
    Calvin.launch(configuration)

    spawn(
      :client,
      fn ->
        # connect to the Sequencer on a non-main replica
        sequencer_B1 = Component.id(_replica=:B, _partition=1, _type=:sequencer)
        client = Client.connect_to(sequencer_B1)

        # send a couple of Transaction requests to the Sequencer
        Client.send_create_tx(client, :a, 1)
        Client.send_create_tx(client, :b, 2)
        Client.send_create_tx(client, :c, 3)
        
        # wait for this epoch to finish
        :timer.sleep(3000)

        # get the key-value stores from every Storage component on the main replica, since
        # the Transaction requests should have been forwarded to that replica
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, configuration.replication_scheme.main_replica)
        )

        # check that every Storage node has the expected key-value store
        # based on the current PartitionScheme with 3 partitions

        # partition 1 Storage should have the `a`, `b`, `c` records
        kv_store_part_1 = Enum.at(kv_stores, 0)
        assert Map.get(kv_store_part_1, :a) == 1
        assert Map.get(kv_store_part_1, :b) == 2
        assert Map.get(kv_store_part_1, :c) == 3

        # partition 2, 3 Storage shouldn't have any data
        kv_store_part_2 = Enum.at(kv_stores, 1)
        kv_store_part_3 = Enum.at(kv_stores, 2)

        assert kv_store_part_2 == %{}
        assert kv_store_part_3 == %{}
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

  test "Requests are eventually replicated to non-main secondary replicas" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration with 3 replicas
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=3, _main_replica=:A), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )
    
    # launch the Calvin components
    Calvin.launch(configuration)

    spawn(
      :client,
      fn ->
        # connect to the Sequencer on the main replica
        sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # send a Transaction with a couple of operations to the Sequencer
        tx = Transaction.new(_operations=[
          Transaction.Op.create(:a, 1),
          Transaction.Op.create(:b, 2)    
        ])
        Client.send_tx(client, tx)
        
        # wait for this epoch to finish
        :timer.sleep(3000)

        # get the key-value stores from every Storage component on the secondary B replica
        # to check if Transactions sent to A were replicated to B and executed against the
        # Storage components
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :B)
        )

        # check that every Storage node has the expected key-value store
        # storage on partition 1 has partition key range of a->m so should
        # contain both `a` and `b`
        kv_store = Enum.at(kv_stores, 0)

        assert Map.get(kv_store, :a) == 1
        assert Map.get(kv_store, :b) == 2

        # storage on partition 2 has partition key range of n->z so shouldn't
        # contain any data
        kv_store = Enum.at(kv_stores, 1)

        assert kv_store == %{}
        assert Map.get(kv_store, :a) == nil
        assert Map.get(kv_store, :b) == nil

        # perform the same check on the C replica
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :C)
        )

        # storage on partition 1 has partition key range of a->m so should
        # contain both `a` and `b`
        kv_store = Enum.at(kv_stores, 0)

        assert Map.get(kv_store, :a) == 1
        assert Map.get(kv_store, :b) == 2

        # storage on partition 2 has partition key range of n->z so shouldn't
        # contain any data
        kv_store = Enum.at(kv_stores, 1)
        assert kv_store == %{}
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

  test "Requests with multi-op Transactions are executed on correct Storage partitions" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=2, _main_replica=:A), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )
    
    # launch the Calvin components
    Calvin.launch(configuration)

    spawn(
      :client,
      fn ->
        # connect to the Sequencer on the main replica
        sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # send a Transaction where both partition 1 and 2 need to participate
        tx = Transaction.new(_operations=[
          Transaction.Op.create(:a, 1),
          Transaction.Op.create(:z, 1)
        ])

        Client.send_tx(client, tx)

        # wait for this epoch to finish
        :timer.sleep(3000)

        # get the key-value stores from the A replica
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :A)
        )

        IO.puts("kv_stores: #{inspect(kv_stores)}")

        # check that every Storage node has the expected key-value store
        # storage on partition 1 should only contain the `a` record since
        # CREATE a->1 operation is local to partition 1 range of [a-m]
        kv_store = Enum.at(kv_stores, 0)

        assert Map.get(kv_store, :a) == 1
        assert Map.get(kv_store, :z) == nil

        # storage on partition 2 should only contain the `z` record since
        # CREATE z->1 operation is local to partition 2 range of [n-z]
        kv_store = Enum.at(kv_stores, 1)

        assert Map.get(kv_store, :z) == 1
        assert Map.get(kv_store, :a) == nil
        
        # get the key-value stores from the B replica
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :B)
        )

        # perform the same checks on the B replica
        # check that every Storage node has the expected key-value store

        # storage on partition 1 should only contain the `a` record since
        # CREATE a->1 operation is local to partition 1 range of [a-m]
        kv_store = Enum.at(kv_stores, 0)

        assert Map.get(kv_store, :a) == 1
        assert Map.get(kv_store, :z) == nil

        # storage on partition 2 should only contain the `z` record since
        # CREATE z->1 operation is local to partition 2 range of [n-z]
        kv_store = Enum.at(kv_stores, 1)

        assert Map.get(kv_store, :z) == 1
        assert Map.get(kv_store, :a) == nil
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
