defmodule TransactionsTest.Multipartition do
  use ExUnit.Case

  doctest Calvin
  doctest PartitionScheme
  doctest ReplicationScheme.Async

  import Emulation, only: [spawn: 2]
  import Kernel, except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3]

  test "Multipartition transactions work as expected" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration with a single replica partitioned over 2 nodes
    num_partitions = 2
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=num_partitions)
    )

    # launch the Calvin components
    Calvin.launch(configuration)

    # set the initial Storage state such that READ requests return valid data
    Calvin.set_storage(configuration, _storage=%{z: 1})

    spawn(
      :client,
      fn ->
        # connect to the Sequencer on the leader replica
        sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # send a Transaction involving both partitions
        tx = Transaction.new(_operations=[
          Transaction.Op.read(:z),
          Transaction.Op.create(:a, Transaction.Expression.new(:z, :+, 1))
        ])
        Client.send_tx(client, tx)
        
        # wait for this epoch to finish
        :timer.sleep(3000)

        # check that storage on partition 1 has the correct state based on the
        # multipartition transaction

        kv_store = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :A)
        ) |> Enum.at(0)

        # `a` should equal `z + 1`
        assert Map.get(kv_store, :a) == 2
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

  test "Multipartition transactions with multiple remote reads work as expected" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration with a single replica partitioned over 2 nodes
    num_partitions = 2
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=num_partitions)
    )

    # launch the Calvin components
    Calvin.launch(configuration)

    # set the initial Storage state such that READ requests return valid data
    Calvin.set_storage(configuration, _storage=%{z: 1, w: 2})

    spawn(
      :client,
      fn ->
        # connect to the Sequencer on the leader replica
        sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # send a Transaction involving both partitions
        tx = Transaction.new(_operations=[
          Transaction.Op.read(:z),
          Transaction.Op.read(:w),
          Transaction.Op.create(:a, Transaction.Expression.new(:z, :+, :w))
        ])
        Client.send_tx(client, tx)
        
        # wait for this epoch to finish
        :timer.sleep(3000)

        # check that storage on partition 1 has the correct state based on the
        # multipartition transaction

        kv_store = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :A)
        ) |> Enum.at(0)

        # `a` should equal `z + w`
        assert Map.get(kv_store, :a) == 3
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

  test "Multipartition transactions with multiple local and remote reads work as expected" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration with a single replica partitioned over 2 nodes
    num_partitions = 2
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=num_partitions)
    )

    # launch the Calvin components
    Calvin.launch(configuration)

    # set the initial Storage state such that READ requests return valid data
    Calvin.set_storage(configuration, _storage=%{z: 1, w: 2, b: 3})

    spawn(
      :client,
      fn ->
        # connect to the Sequencer on the leader replica
        sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # send a Transaction involving both partitions
        tx = Transaction.new(_operations=[
          Transaction.Op.read(:b),
          Transaction.Op.read(:z),
          Transaction.Op.read(:w),
          Transaction.Op.create(:a, Transaction.Expression.new(:z, :+, :w)),
          Transaction.Op.create(:c, Transaction.Expression.new(:b, :+, :w))
        ])
        Client.send_tx(client, tx)
        
        # wait for this epoch to finish
        :timer.sleep(3000)

        # check that storage on partition 1 has the correct state based on the
        # multipartition transaction

        kv_store = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :A)
        ) |> Enum.at(0)

        # `a` should equal `z + w`
        assert Map.get(kv_store, :a) == 3
        # `c` should equal `b + w`
        assert Map.get(kv_store, :c) == 5
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
