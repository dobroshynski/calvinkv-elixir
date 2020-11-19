defmodule SequencerTest do
	use ExUnit.Case
    doctest Sequencer
    
	import Emulation, only: [spawn: 2, send: 2]
	import Kernel,
		except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

	test "Requests to the Sequencer component are logged" do
		Emulation.init()
		Emulation.append_fuzzers([Fuzzers.delay(2)])

		# default replica group and partition single it's only a single node
		sequencer_proc = Sequencer.new(:A, 1)
		sequencer_proc_id = Component.get_id(sequencer_proc)

		IO.puts("created a Sequencer component: #{inspect(sequencer_proc)} with id: #{sequencer_proc_id}")

		# start the node
		spawn(sequencer_proc_id, fn -> Sequencer.start(sequencer_proc) end)

		client = spawn(:client,
			fn -> 
				client = Client.connect_to(sequencer_proc_id)

				# test a ping request to the Sequencer
				Client.ping_sequencer(client)
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
