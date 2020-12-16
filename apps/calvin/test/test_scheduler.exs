defmodule SchedulerTest do
  use ExUnit.Case
  doctest Scheduler
  doctest Sequencer

  import Emulation, only: [spawn: 2]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3]

  test "Sending BatchTransactionMessage to the Scheduler component is logged" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=1)
    )

    # default replica :A and partition 1 since only one "physical" node
    replica = :A
    partition = 1

    # create the Sequencer component and get it's unique id
    sequencer_proc = Sequencer.new(_replica=replica, _partition=partition, configuration)
    sequencer_proc_id = Component.id(sequencer_proc)

    # create the Scheduler component and get it's unique id
    scheduler_proc = Scheduler.new(_replica=replica, _partition=partition, configuration)
    scheduler_proc_id = Component.id(scheduler_proc)

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

  test "Sending BatchTransactionMessage to multiple Scheduler components within a replica is logged" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # create a configuration
    # single replica partitioned across 3 nodes
    num_partitions = 3
    configuration = Configuration.new(
      _replication=ReplicationScheme.Async.new(_num_replicas=1), 
      _partition=PartitionScheme.new(_num_partitions=num_partitions)
    )

    # create as many Sequencer components as there are partitions
    Enum.map(1..num_partitions, 
      fn partition -> 
        sequencer_proc_partitioned = Sequencer.new(_replica=:A, _partition=partition, configuration)
        sequencer_proc_id = Component.id(sequencer_proc_partitioned)

        IO.puts("created Sequencer #{sequencer_proc_id}")

        # spawn each Sequencer
        spawn(sequencer_proc_id, fn -> Sequencer.start(sequencer_proc_partitioned) end)
      end
    )

    # create as many Scheduler components as there are partitions
    Enum.map(1..num_partitions, 
      fn partition -> 
        scheduler_proc_partitioned = Scheduler.new(_replica=:A, _partition=partition, configuration)
        scheduler_proc_id = Component.id(scheduler_proc_partitioned)

        IO.puts("created Scheduler #{scheduler_proc_id}")

        # spawn each Scheduler
        spawn(scheduler_proc_id, fn -> Scheduler.start(scheduler_proc_partitioned) end)
      end
    )
    
    client =
      spawn(
        :client,
        fn ->
          # connect to the Sequencer on partition 1
          default_sequencer = List.to_atom('A1-sequencer')
          client = Client.connect_to(default_sequencer)

          # send a couple of Transaction requests to the Sequencer
          Client.send_create_tx(client, :a, 1)
          Client.send_create_tx(client, :z, 2)

          # wait for this epoch to finish, then send some more requests
          :timer.sleep(3000)

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
end
  