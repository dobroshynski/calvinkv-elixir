defmodule SequencerTest do
  use ExUnit.Case
  doctest Sequencer

  import Emulation, only: [spawn: 2]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3]

  test "Requests to the Sequencer component are logged" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=1)
    )

    # create a Sequencer component and get it's unique id
    sequencer_proc = Sequencer.new(_replica=:A, _partition=1, configuration)
    sequencer_proc_id = Component.id(sequencer_proc)

    # spawn the Sequencer
    spawn(sequencer_proc_id, fn -> Sequencer.start(sequencer_proc) end)

    client =
      spawn(
        :client,
        fn ->
          client = Client.connect_to(sequencer_proc_id)

          # test a ping request to the Sequencer
          Client.ping_sequencer(client)

          # test Transaction requests to the Sequencer
          Client.send_create_tx(client, :a, 1)
          Client.send_create_tx(client, :b, 2)
          Client.send_create_tx(client, :c, 3)

          # check that the Transaction logs at each Sequencer are as expected
          Testing.get_sequencer_states([sequencer_proc_id]) |> Enum.map( 
            fn state -> 
              log = Map.get(state.epoch_logs, state.current_epoch)
              assert length(log) == 3
            end
          )
        end
      )

    # wait for a bit for all requests to be logged
    wait_timeout = 1000

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

  test "Epoch timer messages are logged and epochs incremented" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=1)
    )

    # create a Sequencer component and get it's unique id
    sequencer_proc = Sequencer.new(_replica=:A, _partition=1, configuration)
    sequencer_proc_id = Component.id(sequencer_proc)

    # spawn the Sequencer
    spawn(sequencer_proc_id, fn -> Sequencer.start(sequencer_proc) end)

    client =
      spawn(
        :client,
        fn ->
          client = Client.connect_to(sequencer_proc_id)

          # test a ping request to the Sequencer
          Client.ping_sequencer(client)
          
          # wait for a couple of epochs
          :timer.sleep(5000)

          # check that the epoch at each Sequencer is as expected
          Testing.get_sequencer_states([sequencer_proc_id]) |> Enum.map( 
            fn state ->
              assert state.current_epoch == 3 
            end
          )
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
end
