# A single node / process in the Calvin system
# TODO: currently only supports basic KV operations by
# exposing a simple CRUD in-memory storage interface

# Storage component process for the Calvin system. This is an RSM that 
# keeps a key-value store in memory and can write to disk or 
# take snapshots to persist data, but overall we assume this component
# mimics any CRUD interface that can be substituted in.

defmodule Storage do
  import Emulation, only: [send: 2, whoami: 0]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    type: :storage,
    replica: nil,
    partition: nil,
    # key-value in-memory storage
    store: %{}
  )

  @spec new(atom(), atom()) :: %Storage{}
  def new(replica, partition) do
    %Storage{
      replica: replica,
      partition: partition,
    }
  end

  @doc """
  Returns a unique ID for this component / process, which consists
  of the replica that this component is assigned to, the partition group, and
  the type of component this is. Note: replica + partition uniquely identifies a 
  `physical` node in the Calvin system
  """
  @spec get_id(%Storage{}) :: atom()
  def get_id(proc) do
    replica = to_charlist(proc.replica)
    partition = to_charlist(proc.partition)
    type = to_charlist(proc.type)

    List.to_atom(replica ++ partition ++ '-' ++ type)
  end

  @doc """
  Returns replica + partition which uniquely identifies a 
  `physical` node in the Calvin system
  """
  @spec get_node_id(%Storage{}) :: atom()
  def get_node_id(proc) do
    replica = to_charlist(proc.replica)
    partition = to_charlist(proc.partition)

    List.to_atom(replica ++ partition)
  end
  
  @spec exec_storage_command(%Storage{}, atom(), any(), any()) :: {any() | :ok | :notok, %Storage{}}
  def exec_storage_command(state, command, key, val \\ nil) do
    case command do
      :READ ->
        # if the command is READ, return the value that is the result of the
        # read from the KV store and return the state which doesn't have to be
        # updated in this case

        read_value = Map.get(state.store, key)
        {read_value, state}

      :CREATE ->
        # if the command is CREATE, if the key is new, then update
        # the KV store and return :ok, otherwise do not change anything and
        # return :notok
        
        if Map.has_key?(state.store, key) do
          # indicate an error
          {:notok, state}
        else
          updated_store = Map.put(state.store, key, val)
          updated_state = %{state | store: updated_store}
          {:ok, updated_state}
        end

      :UPDATE ->
        # if the command is UPDATE, if the key already exists, then update
        # the KV store and return :ok, otherwise do not change anything and
        # return :notok

        if Map.has_key?(state.store, key) do
          updated_store = Map.put(state.store, key, val)
          updated_state = %{state | store: updated_store}
          {:ok, updated_state}
        else
          # indicate an error
          {:notok, state}
        end
        
      :DELETE ->
        # if the command is DELETE, remove the value associated with key from the KV 
        # store and return the updated state

        updated_store = Map.delete(state.store, key)
        updated_state = %{state | store: updated_store}
        {:ok, updated_state}

    end
  end

  @doc """
  Run the Storage component RSM, listen for storage execution commands from a Scheduler, and execute 
  operations on the key-value store in memory
  """
  @spec receive_commands(%Storage{}) :: no_return()
  def receive_commands(state) do
    IO.puts("[node #{whoami()}] current state of KV store: #{inspect(state.store)}")   

    receive do
      # raw CRUD messages for the key-value store
      {sender, {:READ, key}} ->
        IO.puts("[node #{whoami()}] received a READ request for key {#{key}}")

        {ret, state} = exec_storage_command(state, :READ, key)

        receive_commands(state)
      {sender, {:CREATE, key, val}} ->
        IO.puts("[node #{whoami()}] received a CREATE request setting key {#{key}} to value {#{val}}")

        {ret, state} = exec_storage_command(state, :CREATE, key, val)

        receive_commands(state)
      {sender, {:UPDATE, key, val}} -> 
        IO.puts("[node #{whoami()}] received a UPDATE request updating key {#{key}} to value {#{val}}")
        
        {ret, state} = exec_storage_command(state, :UPDATE, key, val)

        receive_commands(state)
      {sender, {:DELETE, key}} ->
        IO.puts("[node #{whoami()}] received a DELETE request deleting value associated with key {#{key}}")

        {ret, state} = exec_storage_command(state, :DELETE, key)
        
        receive_commands(state)
      end
  end

  @doc """
  Starts the Storage component RSM
  """
  @spec receive_commands(%Storage{}) :: no_return()
  def start(initial_state) do
    # TODO: do any initializations here for this component / process

    # start accepting storage execution commands
    receive_commands(initial_state)
  end

end

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

  # def exec_storage_command(state, command, key, val) do
  #   case command do
  #     :read ->
  #       store = state.store

  #     :create ->

  #     :update ->

  #     :delete ->

  #   end
  # end

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

        # how can potentially call code to execute the command against a 
        # storage engine
        # {state, result} = exec_storage_command(state, :read, key, nil)

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
    send(node, {:CREATE, key, value})
  end

  @doc """
  Sends an UPDATE request for {key} -> {new_value} to the Calvin node.
  """
  @spec update(%Client{}, any(), any()) :: no_return()
  def update(client, key, new_value) do
    node = client.calvin_node
    send(node, {:UPDATE, key, new_value})
  end

  @doc """
  Sends a READ request for {key} to the Calvin node.
  """
  @spec read(%Client{}, any()) :: no_return()
  def read(client, key) do
    node = client.calvin_node
    send(node, {:READ, key})
  end

  @doc """
  Sends a DELETE request for {key} to the Calvin node.
  """
  @spec delete(%Client{}, any()) :: no_return()
  def delete(client, key) do
    node = client.calvin_node
    send(node, {:DELETE, key})
  end
end
