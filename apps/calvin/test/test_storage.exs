defmodule StorageTest do
  use ExUnit.Case

  doctest Calvin
  doctest Client.Storage

  import Emulation, only: [spawn: 2]

  test "Commands to the Storage component are logged" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])

    # default replica group and partition since it's only a single node
    storage_proc = Storage.new(_replica=:A, _partition=1)
    storage_proc_id = Component.id(storage_proc)

    # start the individual Storage component
    spawn(storage_proc_id, fn -> Storage.start(storage_proc) end)

    # spawn a client and connect to Storage directly
    spawn(
      :client,
      fn ->
        client = Client.Storage.connect_to(_storage=storage_proc_id)

        # perform some operations
        # create a -> 1
        Client.Storage.create(client, :a, 1)
        # create b -> 2
        Client.Storage.create(client, :b, 2)
        # update a -> 2
        Client.Storage.update(client, :a, 2)
        # delete b
        Client.Storage.delete(client, :b)
      end
    )

    # wait for a bit for all requests to be logged
    wait_timeout = 1000

    receive do
    after
      wait_timeout -> :ok
    end
  after
    Emulation.terminate()
  end
end
