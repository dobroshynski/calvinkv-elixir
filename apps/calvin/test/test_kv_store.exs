defmodule SingleKVStoreTest do
	use ExUnit.Case
	doctest Storage
	import Emulation, only: [spawn: 2, send: 2]
	
	import Kernel,
		except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

	test "Client requests to the KV store are logged" do
		Emulation.init()
		Emulation.append_fuzzers([Fuzzers.delay(2)])

		# default replica group and partition single it's only a single node
		storage_node = Storage.new(:A, 1)
		node_id = Storage.get_id(storage_node)

		IO.puts("created a Storage node: #{inspect(storage_node)} with node id #{node_id}")

		# start the node
		spawn(node_id, fn -> Storage.start(storage_node) end)

		client = spawn(:client,
			fn -> 
				client = Client.connect_to(node_id)

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
