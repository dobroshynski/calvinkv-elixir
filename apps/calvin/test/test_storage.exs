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
  
  test "Commands are executed against Storage components by all partitions" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(0)])

    # create a configuration
    # single replica partitioned across 3 nodes
    replica = :A
    num_partitions = 3
    configuration = Configuration.new(_num_replicas=replica, _num_partitions=num_partitions)
    
    # create as many Sequencer components as there are partitions
    Enum.map(1..num_partitions, 
      fn partition -> 
        sequencer_proc_partitioned = Sequencer.new(_replica=replica, _partition=partition, configuration)
        sequencer_proc_id = Component.id(sequencer_proc_partitioned)

        # spawn each Sequencer
        spawn(sequencer_proc_id, fn -> Sequencer.start(sequencer_proc_partitioned) end)
      end
    )

    # create as many Scheduler components as there are partitions
    Enum.map(1..num_partitions, 
      fn partition -> 
        scheduler_proc_partitioned = Scheduler.new(_replica=replica, _partition=partition, configuration)
        scheduler_proc_id = Component.id(scheduler_proc_partitioned)

        # spawn each Scheduler
        spawn(scheduler_proc_id, fn -> Scheduler.start(scheduler_proc_partitioned) end)
      end
    )

    # create as many Storage components as there are partitions
    Enum.map(1..num_partitions, 
    fn partition -> 
      storage_proc_partitioned = Storage.new(_replica=replica, _partition=partition)
      storage_proc_id = Component.id(storage_proc_partitioned)

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

          # get the key-value stores from every Storage component
          partition_view = Configuration.get_partition_view(configuration)
          storage_ids = Enum.map(partition_view, fn partition -> List.to_atom(to_charlist(replica) ++ to_charlist(partition) ++ '-storage') end)

          kv_stores = get_kv_stores(_ids=storage_ids)

          # check that every Storage node has the expected key-value store
          Enum.map(kv_stores,
            fn kv_store ->
              assert Map.get(kv_store, :a) == 1
              assert Map.get(kv_store, :b) == 2
            end
          )

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
