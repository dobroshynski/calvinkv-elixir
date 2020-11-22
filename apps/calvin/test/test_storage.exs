defmodule StorageTest do
  use ExUnit.Case
  doctest Storage

  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Commands to the Storage component are logged" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # default replica group and partition since it's only a single node
    storage_proc = Storage.new(_replica=:A, _partition=1)
    storage_proc_id = Component.id(storage_proc)

    IO.puts(
      "created a Storage component: #{inspect(storage_proc)} with id: #{
        storage_proc_id
      }"
    )

    # start the node
    spawn(storage_proc_id, fn -> Storage.start(storage_proc) end)

    client =
      spawn(
        :client,
        fn ->
          client = Client.connect_to(storage_proc_id)

          # perform some operations
          # create a -> 1
          Client.create(client, :a, 1)
          # create b -> 2
          Client.create(client, :b, 2)
          # update a -> 2
          Client.update(client, :a, 2)
          # delete b
          Client.delete(client, :b)
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
  
  test "Commands are executed against Storage components by all partitions" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(0)])

    # create a configuration
    # single replica partitioned across 3 nodes
    num_partitions = 3
    configuration = Configuration.new(_num_replicas=1, _num_partitions=num_partitions)
    
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

    # create as many Storage components as there are partitions
    Enum.map(1..num_partitions, 
    fn partition -> 
      storage_proc_partitioned = Storage.new(_replica=:A, _partition=partition)
      storage_proc_id = Component.id(storage_proc_partitioned)

      IO.puts("created Storage #{storage_proc_id}")

      # spawn each Storage
      spawn(storage_proc_id, fn -> Storage.start(storage_proc_partitioned) end)
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
