defmodule SchedulerTest do
  use ExUnit.Case
  doctest Scheduler
  doctest Sequencer

  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "BatchTransactionMessage to the Scheduler component are logged" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration
    configuration = Configuration.new(_num_replicas=1, _num_partitions=1)

    # default replica :A and partition 1 since only one "physical" node
    replica = :A
    partition = 1

    # create the Sequencer component and get it's unique id
    sequencer_proc = Sequencer.new(_replica=replica, _partition=partition, configuration)
    sequencer_proc_id = Component.get_id(sequencer_proc)

    # create the Scheduler component and get it's unique id
    scheduler_proc = Scheduler.new(_replica=replica, _partition=partition, configuration)
    scheduler_proc_id = Component.get_id(scheduler_proc)

    IO.puts("created Sequencer #{sequencer_proc_id} and Scheduler #{scheduler_proc_id}")
      
    # start the nodes
    spawn(sequencer_proc_id, fn -> Sequencer.start(sequencer_proc) end)
    spawn(scheduler_proc_id, fn -> Scheduler.start(scheduler_proc) end)
    
    client =
      spawn(
        :client,
        fn ->
          client = Client.connect_to(sequencer_proc_id)

          # send a couple of Transaction requests to the Sequencer
          Client.send_create_tx(client, :a, 1)
          Client.send_create_tx(client, :b, 2)

          # wait for this epoch to finish, then send some more requests
          :timer.sleep(3000)

          Client.send_create_tx(client, :c, 3)

        end
      )

    # wait for the first epoch to finish and the BatchTransactionMessage
    # to be sent to the Scheduler component
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
  