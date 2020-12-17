defmodule BalanceTransferExperiment do
  use ExUnit.Case

  doctest Calvin

  import Emulation, only: [spawn: 2]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @tag timeout: :infinity
  test "run multiple 'balance transfer' transactions" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(10)])

    # create a configuration with a single replica
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=2)
    )
    # launch the Calvin components
    Calvin.launch(configuration)
    # set the initial Storage state such that READ requests return valid data
    Calvin.set_storage(configuration, _storage=%{Alice: 100, Zoe: 100})

    spawn(
      :client,
      fn ->
        # connect to the Sequencer on the leader replica
        sequencer = Component.id(_replica=:A, _partition=1, _type=:sequencer)
        client = Client.connect_to(sequencer)

        # create a balance transfer Transaction
        tx = Transaction.new(_operations=[
          Transaction.Op.update(:Alice, Transaction.Expression.new(:Alice, :-, 1)),
          Transaction.Op.update(:Zoe, Transaction.Expression.new(:Zoe, :+, 1))
        ])

        # send all Transactions one after another
        num_transactions = 20

        Enum.map(1..num_transactions,
          fn _ ->
            Client.send_tx(client, tx)

            # wait in-between client requests
            :timer.sleep(1)
          end
        )

        # give some time for all of the Transactions to execute
        :timer.sleep(3000)

        # check the Storage state after all of the Transactions are executed
        kv_stores = Testing.get_kv_stores(
          _ids=Configuration.get_storage_view(configuration, :A)
        )

        kv_store_part1 = Enum.at(kv_stores, 0)
        kv_store_part2 = Enum.at(kv_stores, 1)

        assert Map.get(kv_store_part1, :Alice) == 80
        assert Map.get(kv_store_part2, :Zoe) == 120
      end
    )

    wait_timeout = 5000

    receive do
    after
      wait_timeout ->
        Emulation.terminate()
        :ok
    end
  end
end
