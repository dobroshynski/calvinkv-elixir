# Module for an arbitrary client that can connect to a Calvin deployment via a
# Sequencer component of the system, which is responsible for collecting Transaction
# requests

defmodule Client do
  @enforce_keys [:sequencer]

  import Emulation, only: [send: 2]
  import Kernel, except: [send: 2]

  defstruct(sequencer: nil)

  @doc """
  Constructs a new Client that will 'connect to' and be able to send 
  Transaction requests to the Sequencer with the given `sequencer_id`
  """
  @spec connect_to(atom()) :: %Client{sequencer: atom()}
  def connect_to(sequencer_id) do
    %Client{sequencer: sequencer_id}
  end

  @doc """
  Sends a ping request to the connected Sequencer component
  """
  @spec ping_sequencer(%Client{}) :: no_return()
  def ping_sequencer(client) do
    send(client.sequencer, :ping)
  end

  @doc """
  Sends a given Transaction request to the connected Sequencer 
  component
  """
  @spec send_tx(%Client{}, %Transaction{}) :: no_return()
  def send_tx(client, tx) do
    # mark the Transcation as started since the client is sending
    # the request
    time = DateTime.utc_now()
    tx = Transaction.set_started(tx, _time=time)

    send(client.sequencer, tx)
  end

  @doc """
  Creates and sends a Transaction with a single CREATE operation of
  creating {key -> val} to the connected Sequencer component
  """
  @spec send_create_tx(%Client{}, atom(), atom()) :: no_return()
  def send_create_tx(client, key, val) do
    tx = Transaction.new(_operations=[Transaction.Op.create(key, val)])
    Client.send_tx(client, _transaction=tx)
  end

  @doc """
  Creates and sends a Transaction with a single UPDATE operation of
  updating {key -> val} to the connected Sequencer component
  """
  @spec send_update_tx(%Client{}, atom(), atom()) :: no_return()
  def send_update_tx(client, key, val) do
    tx = Transaction.new(_operations=[Transaction.Op.update(key, val)])
    Client.send_tx(client, _transaction=tx)
  end
  
  @doc """
  Creates and sends a Transaction with a single DELETE operation of
  deleting entry {key} to the connected Sequencer component
  """
  @spec send_delete_tx(%Client{}, atom()) :: no_return()
  def send_delete_tx(client, key) do
    tx = Transaction.new(_operations=[Transaction.Op.delete(key)])
    Client.send_tx(client, _transaction=tx)
  end
end

# Module for a client that can connect directly to a Storage component
# within a Calvin deployment send individual operations to be executed

defmodule Client.Storage do
  @enforce_keys [:storage]

  import Emulation, only: [send: 2]
  import Kernel, except: [send: 2]

  defstruct(storage: nil)

  @doc """
  Constructs a new Client.Storage connecting to the `storage_id` Storage
  component
  """
  @spec connect_to(atom()) :: %Client.Storage{storage: atom()}
  def connect_to(storage_id) do
    %Client.Storage{storage: storage_id}
  end

  @doc """
  Sends a CREATE request for {key} -> {value} to the Storage component
  """
  @spec create(%Client.Storage{}, any(), any()) :: no_return()
  def create(client, key, value) do
    send(client.storage, {:CREATE, key, value})
  end

  @doc """
  Sends an UPDATE request for {key} -> {new_value} to the Storage component
  """
  @spec update(%Client.Storage{}, any(), any()) :: no_return()
  def update(client, key, new_value) do
    send(client.storage, {:UPDATE, key, new_value})
  end

  @doc """
  Sends a READ request for {key} to the Storage component
  """
  @spec read(%Client.Storage{}, any()) :: no_return()
  def read(client, key) do
    send(client.storage, {:READ, key})
  end

  @doc """
  Sends a DELETE request for {key} to the Storage component
  """
  @spec delete(%Client.Storage{}, any()) :: no_return()
  def delete(client, key) do
    send(client.storage, {:DELETE, key})
  end
end
