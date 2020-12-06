defmodule StorageTest.Raft do
  use ExUnit.Case

  doctest Calvin
  doctest PartitionScheme
  doctest ReplicationScheme.Raft

  import Emulation, only: [spawn: 2, send: 2]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  # Helper function to collect the key-value store states of Storage components given 
  # a list of Storage unique ids
  defp get_kv_stores(storage_ids) do
    Enum.map(storage_ids,
      fn id ->
        # send testing / debug message to the Storage component directly
        send(id, :get_kv_store)
      end
    )
    Enum.map(storage_ids,
      fn id ->
        receive do
          {^id, kv_store} -> kv_store
        end
      end
    )
  end
  
  test "ReplicationScheme.Raft configuration works as expected" do
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
        # connect to the Sequencer on the main replica
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

        kv_stores = get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :A)
        )

        kv_store_part_1 = Enum.at(kv_stores, 0)
        assert Map.get(kv_store_part_1, :a) == 1
        assert Map.get(kv_store_part_1, :z) == nil

        kv_store_part_2 = Enum.at(kv_stores, 1)
        assert Map.get(kv_store_part_2, :a) == nil
        assert Map.get(kv_store_part_2, :z) == 1

        # perform the same check on replica B
        kv_stores = get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :B)
        )

        kv_store_part_1 = Enum.at(kv_stores, 0)
        assert Map.get(kv_store_part_1, :a) == 1
        assert Map.get(kv_store_part_1, :z) == nil

        kv_store_part_2 = Enum.at(kv_stores, 1)
        assert Map.get(kv_store_part_2, :a) == nil
        assert Map.get(kv_store_part_2, :z) == 1

        # perform the same check on replica C
        kv_stores = get_kv_stores(
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
end
