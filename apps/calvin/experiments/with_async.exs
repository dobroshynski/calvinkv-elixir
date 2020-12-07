defmodule LatencyExperiment.Async do
  use ExUnit.Case

  doctest Calvin

  import Emulation, only: [spawn: 2]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @tag timeout: :infinity
  test "run multiple latencies experiment with Async replication" do

    Enum.map(0..15, 
      fn val ->
        latency = val * 10

        IO.puts("[latency = #{latency}]\n----------------------")
        Emulation.init()

        # delay messages across sequencers
        Emulation.append_fuzzers([Fuzzers.delay(latency)])
    
        # create a configuration using async replication
        configuration = Configuration.new(
          _replication=ReplicationScheme.Async.new(_num_replicas=5), 
          _partition=PartitionScheme.new(_num_partitions=2)
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
            tx = Transaction.new(
              _operations=[
                Transaction.Op.create(:a, 1),
                Transaction.Op.create(:z, 1)
              ],
              _id=:tx1
            )
            Client.send_tx(client, tx)
          end
        )
    
        # timeout after 10sec per run
        wait_timeout = 10_000
    
        receive do
        after
          wait_timeout -> 
            Emulation.terminate()
            :ok
        end
      end
    )
  end

  test "run single latency experiment with Async replication" do
    Emulation.init()

    latency = 10
    IO.puts("[latency = #{latency}]\n----------------------")

    # delay messages across sequencers
    Emulation.append_fuzzers([Fuzzers.delay(latency)])

    # create a configuration using async replication
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=5), 
      _partition=PartitionScheme.new(_num_partitions=2)
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
        tx = Transaction.new(
          _operations=[
            Transaction.Op.create(:a, 1),
            Transaction.Op.create(:z, 1)
          ],
          _id=:tx1
        )
        Client.send_tx(client, tx)
      end
    )

    # timeout after 10sec per run
    wait_timeout = 10_000

    receive do
    after
      wait_timeout -> 
        Emulation.terminate()
        :ok
    end
  end
end
