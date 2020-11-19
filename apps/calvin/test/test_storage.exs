defmodule StorageTest do
	use ExUnit.Case
    doctest Storage
    
	import Emulation, only: [spawn: 2, send: 2]
	import Kernel,
		except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

	test "Commands to the Storage component are logged" do
		Emulation.init()
		Emulation.append_fuzzers([Fuzzers.delay(2)])

		# default replica group and partition single it's only a single node
		storage_proc = Storage.new(:A, 1)
		storage_proc_id = Component.get_id(storage_proc)

		IO.puts("created a Storage component: #{inspect(storage_proc)} with id: #{storage_proc_id}")

		# start the node
		spawn(storage_proc_id, fn -> Storage.start(storage_proc) end)

		client = spawn(:client,
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
end
