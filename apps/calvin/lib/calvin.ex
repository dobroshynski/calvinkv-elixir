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

  @doc """
  Creates a Storage RSM in default initial configuration
  """
  @spec new(atom(), atom()) :: %Storage{}
  def new(replica, partition) do
    %Storage{
      replica: replica,
      partition: partition,
    }
  end
  
  @doc """
  Function that processes and executes a given CRUD command against the Storage's key-value store.
  Depending on the operation, returns either just an :ok, :notok, or the value in case of READ request, 
  in addition to the updated state for the Storage RSM
  """
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
  @spec start(%Storage{}) :: no_return()
  def start(initial_state) do
    # TODO: do any initializations here for this component / process

    # start accepting storage execution commands
    receive_commands(initial_state)
  end
end

# Sequencer component process for the Calvin system. This is an RSM that 
# receives requests from clients during individual epochs, appends them locally
# to a log-like structure, and when the epoch is completed, replicates the inputs
# before forwarding them to the Scheduler layer for execution

defmodule Sequencer do
  import Emulation, only: [send: 2, timer: 1, whoami: 0]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    type: :sequencer,
    replica: nil,
    partition: nil,

    epoch_timer_duration: 2000, # TODO: make 2 seconds for now
    epoch_timer: nil,

    current_epoch: nil,
    # local logs of entries for epochs
    # this is a map of {epoch number => log of requests for that epoch}
    epoch_logs: %{}
  )

  @doc """
  Creates a Sequencer RSM in default initial configuration
  """
  @spec new(atom(), atom()) :: %Sequencer{}
  def new(replica, partition) do
    %Sequencer{
      replica: replica,
      partition: partition,
      current_epoch: 0
    }
  end

  @doc """
  Extend the local log of the Sequencer RSM to include an incoming Transaction
  """
  @spec add_to_log(%Sequencer{}, %Transaction{}) :: %Sequencer{}
  def add_to_log(state, transaction) do
    current_epoch = state.current_epoch
    current_epoch_log = Map.get(state.epoch_logs, current_epoch)

    # extend the log for this epoch with the incoming Transaction
    updated_log = current_epoch_log ++ [transaction]

    # update the map of {epoch number -> log of tx requests for that epoch}
    updated_epoch_logs = Map.put(state.epoch_logs, current_epoch, updated_log)

    %{state | epoch_logs: updated_epoch_logs}
  end

  @doc """
  Sends out BatchTransactionMessage messages to all Schedulers on every parition 
  within the same replica as the current Sequencer process / component
  """
  @spec send_current_epoch_batch(%Sequencer{}) :: no_return()
  def send_current_epoch_batch(state) do
    batch_tx_msg = %BatchTransactionMessage{
      sequencer_id: whoami(),
      epoch: state.current_epoch,
      # log of Transactions for the current epoch
      batch: Map.get(state.epoch_logs, state.current_epoch)
    }

    IO.puts("[node #{whoami()}] created a BatchTransactionMessage: #{inspect(batch_tx_msg)}")

    # TODO: send out the BatchTransactionMessage to all Schedulers within the same replica as the
    # current Sequencer process / component 

    # for now, send to the single Scheduler on the same "physical machine" as the current Sequencer
    # process. Assume the replica is :A and partition is 1 which is the same as current process
    scheduler_id = List.to_atom(to_charlist(state.replica) ++ to_charlist(state.partition) ++ '-' ++ to_charlist(:scheduler))
    send(scheduler_id, batch_tx_msg)

    IO.puts("[node #{whoami()}] sent BatchTransactionMessage to scheduler process #{scheduler_id}")
  end

  @doc """
  Run the Sequencer component RSM, listen for client requests, append them to log and keep track of
  current epoch timer
  """
  @spec receive_requests(%Sequencer{}) :: no_return()
  def receive_requests(state) do
    receive do
      # client requests
      # TODO: extend this to transaction requests or CRUD requests for KV store
      {client_sender, tx = %Transaction{
        type: type,
        key: key,
        val: val
      }} ->
        IO.puts("[node #{whoami()}] received a Transaction request: #{inspect(tx)} from client {#{client_sender}}")
        
        # timestamp this Transaction at time of receiving
        tx = Transaction.add_timestamp(tx)
        IO.puts("[node #{whoami()}] tx updated to #{inspect(tx)}")

        # add the incoming Transaction to the Sequencer's local log
        state = add_to_log(state, tx)
        IO.puts("[node #{whoami()}] local log for epoch #{state.current_epoch} updated to #{inspect(Map.get(state.epoch_logs, state.current_epoch))}")

        # continue listening for requests
        receive_requests(state)

      {client_sender, :ping} ->
        IO.puts("[node #{whoami()}] received a ping request from client {#{client_sender}}")

        # continue listening for requests
        receive_requests(state)

      # epoch timer message signifying the end of the current epoch
      :timer ->
        IO.puts("[node #{whoami()}] epoch #{state.current_epoch} has ended, sending BatchTransactionMessage to Schedulers, then starting new epoch")

        # send out a BatchTransactionMessage to the Scheduler
        send_current_epoch_batch(state)

        # increment the epoch from current -> current + 1
        state = increment_epoch(state)

        # continue listening for requests
        receive_requests(state)
    end
  end

  @doc """
  Updates the Sequencer RSM to increment the current epoch by 1 and create a new empty log
  for that epoch to hold client requests
  """
  @spec increment_epoch(%Sequencer{}) :: %Sequencer{}
  def increment_epoch(state) do
    # increment the epoch in the RSM
    old_epoch = state.current_epoch
    state = %{state | current_epoch: old_epoch + 1}
    IO.puts("-------------------- EPOCH #{state.current_epoch} --------------------")
    IO.puts("[node #{whoami()}] incremented epoch from #{old_epoch} -> #{state.current_epoch}")

    # create an empty log for the current updated epoch in the RSM
    updated_epoch_logs = Map.put(state.epoch_logs, state.current_epoch, [])
    state = %{state | epoch_logs: updated_epoch_logs}

    IO.puts("[node #{whoami()}] current state of `epoch_logs`: #{inspect(state.epoch_logs)}")

    # start the timer for duration of epoch
    new_epoch_timer = timer(state.epoch_timer_duration)
    state = %{state | epoch_timer: new_epoch_timer}
    
    state
  end

  @doc """
  Starts the Sequencer component RSM
  """
  @spec start(%Sequencer{}) :: no_return()
  def start(initial_state) do
    # increment the epoch from 0 -> 1
    state = increment_epoch(initial_state)

    # start accepting requests from clients
    receive_requests(state)
  end
end

# Scheduler component process for the Calvin system. This is an RSM that 
# receives batched requests via a message from the Sequencer components, and
# is then responsible for interleaving batches from different partitions from the
# same replica if necessary, and executing the transactions in the final total order
# against the Storage component. The Scheduler 'schedules' tx execution in a deterministic
# manner in the order that it determines the transactions to be in for a given epoch

defmodule Scheduler do
  import Emulation, only: [send: 2, timer: 1, whoami: 0]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    type: :scheduler,
    replica: nil,
    partition: nil
  )

  @doc """
  Creates a Scheduler RSM in default initial configuration
  """
  @spec new(atom(), atom()) :: %Scheduler{}
  def new(replica, partition) do
    %Scheduler{
      replica: replica,
      partition: partition
    }
  end

  @doc """
  Run the Scheduler component RSM, listen for messages with batched transaction requests from 
  Sequencer components, store them and once received all of the expected BatchTransactionMessage
  messages, execute the transaction requests against the Storage component
  """
  @spec receive_batched_transaction_messages(%Scheduler{}) :: no_return()
  def receive_batched_transaction_messages(state) do
    receive do
      {sender, msg = %BatchTransactionMessage{
        sequencer_id: sequencer_id,
        epoch: epoch,
        batch: batch
      }} ->
        IO.puts("[node #{whoami()}] received a BatchTransactionMessage from sequencer node #{sender}")
        IO.puts("[node #{whoami()}] msg received: #{inspect(msg)}")

        # continue receiving BatchTransactionMessage
        receive_batched_transaction_messages(state)
    end
  end

  @doc """
  Starts the Scheduler component RSM
  """
  @spec start(%Scheduler{}) :: no_return()
  def start(initial_state) do
    # TODO: do any initializations here for this component / process

    # start accepting BatchTransactionMessage messages from the Sequencer components
    receive_batched_transaction_messages(initial_state)
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

# Generic component module for Calvin components. Implements some re-used functions
# that all components may use.

defmodule Component do
  require Storage
  require Sequencer
  require Scheduler

  @doc """
  Returns a unique ID for this component / process, which consists
  of the replica that this component is assigned to, the partition group, and
  the type of component this is. Note: replica + partition uniquely identifies a 
  `physical` node in the Calvin system
  """
  @spec get_id(%Storage{} | %Sequencer{} | %Scheduler{}) :: atom()
  def get_id(proc) do
    replica = to_charlist(proc.replica)
    partition = to_charlist(proc.partition)
    type = to_charlist(proc.type)

    List.to_atom(replica ++ partition ++ '-' ++ type)
  end

  @doc """
  Returns replica + partition for this process which uniquely identifies a 
  `physical` node that this process belongs to in the Calvin system
  """
  @spec get_node_id(%Storage{} | %Sequencer{} | %Scheduler{}) :: atom()
  def get_node_id(proc) do
    replica = to_charlist(proc.replica)
    partition = to_charlist(proc.partition)

    List.to_atom(replica ++ partition)
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

  @doc """
  Sends a ping request to a Sequencer component of a Calvin node.
  """
  def ping_sequencer(client) do
    send(client.calvin_node, :ping)
  end

  def send_create_tx(client, key, val) do
    node = client.calvin_node
    tx = Transaction.create(key, val)

    send(node, tx)
  end
end
