defmodule TransactionsTest.Invariants do
  use ExUnit.Case

  doctest Calvin
  doctest PartitionScheme
  doctest ReplicationScheme.Async

  import Emulation, only: [spawn: 2]
  import Kernel, except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3]

  test "Transactions deterministically abort based on the given invariant operation" do
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
          
        # send a Transaction with an invariant that should evaluate to `false`
        # and abort before executing writes to `:a`
        tx_1 = Transaction.new(_operations=[
          Transaction.Op.invariant(
            Transaction.Expression.new(:z, :==, 0)
          ),
          Transaction.Op.create(:a, Transaction.Expression.new(:z, :+, 1))
        ])
        Client.send_tx(client, tx_1)

        # wait to send the next transaction
        :timer.sleep(1)

        # send a Transaction with an invariant that should evaluate to `true`
        # and proceed to apply the write to `:a`
        tx_2 = Transaction.new(_operations=[
          Transaction.Op.invariant(
            Transaction.Expression.new(:z, :>, 0)
          ),
          Transaction.Op.create(:a, Transaction.Expression.new(:z, :-, 1))
        ])
        Client.send_tx(client, tx_2)
        
        # wait for this epoch to finish
        :timer.sleep(3000)

        # check that storage on partition 1 has the correct state with
        # only Transaction's `tx2` write operation applied

        kv_store = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :A)
        ) |> Enum.at(0)

        # `a` should equal `z - 1` since only `tx2` write operation
        # should be applied
        assert Map.get(kv_store, :a) == 0
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

  test "Multipartition balance transfer succeeds with invariant" do
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
    Calvin.set_storage(configuration, _storage=%{Alice: 100, Bob: 50, Zoe: 100})

    spawn(
      :client,
      fn ->
        # connect to the Sequencer on the leader replica
        sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # send a 'balance transfer' Transaction involving both partitions
        # transfering 20 units from account of `Alice` stored on partition 1 
        # to `Zoe` stored on partition 2 and with an invariant such that
        # the balance of `Alice` account cannot go lower than 0

        amount = 20
        
        tx = Transaction.new(_operations=[
          Transaction.Op.invariant(
            Transaction.Expression.new(:Alice, :>=, amount)
          ),
          Transaction.Op.update(:Alice, Transaction.Expression.new(:Alice, :-, amount)),
          Transaction.Op.update(:Zoe, Transaction.Expression.new(:Zoe, :+, amount))
        ])
        Client.send_tx(client, tx)
        
        # wait for this epoch to finish
        :timer.sleep(3000)

        # check that Storage states are as expected after the balance transfer
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :A)
        )

        kv_store_part1 = Enum.at(kv_stores, 0)
        kv_store_part2 = Enum.at(kv_stores, 1)

        assert Map.get(kv_store_part1, :Alice) == 80
        assert Map.get(kv_store_part2, :Zoe) == 120
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

    test "Multipartition balance transfer aborts with invariant" do
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
    Calvin.set_storage(configuration, _storage=%{Alice: 100, Bob: 50, Zoe: 100})

    spawn(
      :client,
      fn ->
        # connect to the Sequencer on the leader replica
        sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # send a 'balance transfer' Transaction involving both partitions
        # attempting to transfer more units than available from account of
        # `Alice` stored on partition 1  to `Zoe` stored on partition 2 and
        # with an invariant such that the balance of `Alice` account cannot
        # go lower than 0

        amount = 101
        
        tx = Transaction.new(_operations=[
          Transaction.Op.invariant(
            Transaction.Expression.new(:Alice, :>=, amount)
          ),
          Transaction.Op.update(:Alice, Transaction.Expression.new(:Alice, :-, amount)),
          Transaction.Op.update(:Zoe, Transaction.Expression.new(:Zoe, :+, amount))
        ])
        Client.send_tx(client, tx)

        # wait for this epoch to finish
        :timer.sleep(3000)

        # check that Storage state balances are unchaged since the transaction
        # should abort before any writes are applied
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :A)
        )

        kv_store_part1 = Enum.at(kv_stores, 0)
        kv_store_part2 = Enum.at(kv_stores, 1)

        assert Map.get(kv_store_part1, :Alice) == 100
        assert Map.get(kv_store_part2, :Zoe) == 100
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
