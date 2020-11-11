# A single node / process in the Calvin system
# TODO: currently only supports basic KV operations by
# exposing a simple CRUD in-memory storage interface

defmodule CalvinNode do
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  require Logger

  # What a CalvinNode process requires as it's state
  defstruct(
    # The replica this node belongs to
    # letter from a -> z
    replica: nil,
    # The partition this node belongs to
    # number from 1 -> some upper bound
    partition: nil,

    # The key-value in-memory storage for this node
    store: %{}
  )

  @spec new(atom(), atom()) :: %CalvinNode{}
  def new(replica, partition) do
    %CalvinNode{
      replica: replica,
      partition: partition,
    }
  end

  @spec node_id(%CalvinNode{}) :: atom()
  def node_id(node) do
    replica = to_charlist(node.replica)
    partition = to_charlist(node.partition)
    List.to_atom(replica ++ partition)
  end

  @doc """
  Function to run the Calvin node as a state machine and start listening to 
  messages
  """
  @spec run(%CalvinNode{}) :: no_return()
  def run(state) do
    # IO.puts("current state of KV store: #{inspect(state.store)}")   
     
    receive do
      # messages from external clients
      {sender, {:read, key}} ->
        IO.puts("[node #{whoami()}] received a READ request for key {#{key}}")

        run(state)
      {sender, {:create, key, val}} ->
        IO.puts("[node #{whoami()}] received a CREATE request setting key {#{key}} to value {#{val}}")

        run(state)
      {sender, {:update, key, val}} -> 
        IO.puts("[node #{whoami()}] received a UPDATE request updating key {#{key}} to value {#{val}}")

        run(state)
      {sender, {:delete, key}} ->
        IO.puts("[node #{whoami()}] received a DELETE request deleting value associated with key {#{key}}")
        
        run(state)
      end
  end
end

defmodule Client do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @moduledoc """
  A client that can be used to connect and send
  requests to a CalvinNode
  """
  alias __MODULE__
  @enforce_keys [:calvin_node]
  defstruct(calvin_node: nil)

  @doc """
  Construct a new Calvin Client. This takes an ID of
  any CalvinNode that is able to take requests. Different clients can be 
  given different CalvinNode id's (perhaps by a load balancer of some sorts?) 
  and we let the Calvin system to figure out how to agree on and execute the 
  requests
  """
  @spec connect_to(atom()) :: %Client{calvin_node: atom()}
  def connect_to(node_id) do
    %Client{calvin_node: node_id}
  end

  @doc """
  Sends a CREATE request for {key} -> {value} to the Calvin node.
  """
  @spec create(%Client{}, any(), any()) :: no_return()
  def create(client, key, value) do
    node = client.calvin_node
    send(node, {:create, key, value})
  end

  @doc """
  Sends an UPDATE request for {key} -> {new_value} to the Calvin node.
  """
  @spec update(%Client{}, any(), any()) :: no_return()
  def update(client, key, new_value) do
    node = client.calvin_node
    send(node, {:update, key, new_value})
  end

  @doc """
  Sends a READ request for {key} to the Calvin node.
  """
  @spec read(%Client{}, any()) :: no_return()
  def read(client, key) do
    node = client.calvin_node
    send(node, {:read, key})
  end

  @doc """
  Sends a DELETE request for {key} to the Calvin node.
  """
  @spec delete(%Client{}, any()) :: no_return()
  def delete(client, key) do
    node = client.calvin_node
    send(node, {:delete, key})
  end
end
