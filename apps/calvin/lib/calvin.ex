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

      # ----------------------------
      # testing / debugging messages
      # ----------------------------

      {debug_sender, :get_kv_store} ->
        send(debug_sender, state.store)
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

    # deployment Configuration
    configuration: nil,

    epoch_timer_duration: 2000, # TODO: make 2 seconds for now
    epoch_timer: nil,

    current_epoch: nil,
    # local logs of entries for epochs
    # this is a map of {epoch number => log of requests for that epoch}
    epoch_logs: %{}
  )

  @doc """
  Creates a Sequencer RSM in default initial configuration and with deployment
  Configuration specified by `configuration`
  """
  @spec new(atom(), atom(), %Configuration{}) :: %Sequencer{}
  def new(replica, partition, configuration) do
    %Sequencer{
      replica: replica,
      partition: partition,
      configuration: configuration,
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

    # get all of the partition numbers in a replica via the current Configuration
    partition_view = Configuration.get_partition_view(state.configuration)
    IO.puts("[node #{whoami()}] list of partitions per replica: #{inspect(partition_view)}")


    # send out the BatchTransactionMessage to all Schedulers within the same replica as the
    # current Sequencer process / component 
    Enum.map(partition_view, 
      fn partition ->
        # construct a unique id for a recipient Scheduler component within the current replica
        # given a partition
        scheduler_id = List.to_atom(to_charlist(state.replica) ++ to_charlist(partition) ++ '-' ++ to_charlist(:scheduler))
        
        # send a BatchTransactionMessage to this specific Scheduler component
        send(scheduler_id, batch_tx_msg)

        IO.puts("[node #{whoami()}] sent BatchTransactionMessage to scheduler process #{scheduler_id}")
      end
    )    
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

      # ----------------------------
      # testing / debugging messages
      # ----------------------------

      {debug_sender, :get_state} ->
        send(debug_sender, state)
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
    partition: nil,

    # locally storing batches that the Scheduler receives from `n` Sequencer components
    # as partial orderings of Transactions
    # {epoch -> [ [tx batch 1], [tx batch 2], ... , [tx batch n] ]}
    partial_orders: %{},
    # global orders of Transactions once the interleaving is done
    # {epoch -> [tx1, tx2, ..., tx_n]}
    global_orders: %{},

    # epoch for which the Scheduler is interleaving partial order Transaction batches for
    processing_epoch: 0,

    # deployment Configuration
    configuration: nil
  )

  @doc """
  Creates a Scheduler RSM in default initial configuration and with deployment
  Configuration specified by `configuration`
  """
  @spec new(atom(), atom(), %Configuration{}) :: %Scheduler{}
  def new(replica, partition, configuration) do
    %Scheduler{
      replica: replica,
      partition: partition,
      configuration: configuration
    }
  end

  @doc """
  Saves a received batch of Transactions via a BatchTransactionMessage from a Sequencer 
  component to the Scheduler RSM state. Batches are recorded as lists of partial orderings
  per specific epoch (which is also sent via the BatchTransactionMessage). Once batches from
  all partitions arrive, the Scheduler will be able to interleave the batches into one global 
  order and execute against the Storage component
  """
  @spec save_received_batch(%Scheduler{}, %BatchTransactionMessage{}) :: %Scheduler{}
  def save_received_batch(state, batch_msg) do
    epoch = batch_msg.epoch
    batch = batch_msg.batch

    # get the partial order batches for this epoch received so far and append the new batch
    tx_batches = Map.get(state.partial_orders, epoch, [])
    updated_tx_batches = tx_batches ++ [batch]

    state = %{state | partial_orders: Map.put(state.partial_orders, epoch, updated_tx_batches)}

    IO.puts("[#{whoami()}] PARTIAL TX ORDERS (epoch #{epoch}): 
    #{inspect(state.partial_orders)}")
    state
  end

  @doc """
  Given a set of partial ordering batches of Transactions and an epoch for which the Scheduler
  is attempting to interleave partial orderings for, returns an interleaved (sorted) list of
  Transactions based on tx timestamp
  """
  @spec interleave_partial_orderings([[%Transaction{}]], non_neg_integer()) :: [%Transaction{}]
  def interleave_partial_orderings(partial_orders, epoch) do
    # get all of the Transactions from all partial ordering batches into one list,
    # since `partial_orders` is a list of lists containing Transaction batches
    all_txs = List.flatten(Map.get(partial_orders, epoch))

    IO.puts("[node #{whoami()}] all txs for epoch #{epoch} flattened: #{inspect(all_txs)}")
    
    # sort all of the Transactions according to their timestamps
    sorted_txs = Enum.sort(all_txs, 
      fn tx1, tx2 ->
        if tx1.timestamp < tx2.timestamp do
          true
        else
          false
        end
      end
    )

    IO.puts("[node #{whoami()}] GLOBAL TX order done:
     #{inspect(sorted_txs)}")
    
    sorted_txs
  end

  @doc """
  Attempts to process the partial orderings of Transaction batches received so far for the current `state.processing_epoch`
  by interleaving the partial orders into a global order of transactions. Returns the Scheduler state unchaged if haven't received
  the batches from all Sequencers from all expected partitions yet or returns the Scheduler state updated with the global Transaction
  order set in `global_orders` for the current `processing_epoch`
  """
  @spec attempt_tx_interleave(%Scheduler{}) :: %Scheduler{}
  def attempt_tx_interleave(state) do
    expected = state.configuration.num_partitions
    received = length(Map.get(state.partial_orders, state.processing_epoch, []))

    IO.puts("[node #{whoami()}] attempting to interleave txs for epoch #{state.processing_epoch}
    partial order state: #{inspect(Map.get(state.partial_orders, state.processing_epoch))}, 
    batches expected: #{expected}, 
    batches received: #{received}")

    if expected != received do   
      # return the state unchanged
      state
    else
      ordered_txs = interleave_partial_orderings(state.partial_orders, state.processing_epoch)

      # save the ordered global Transaction ordering for the epoch that is currently
      # being processed by this Scheduler
      %{state | global_orders: Map.put(state.global_orders, state.processing_epoch, ordered_txs)}
    end
  end

  @doc """
  Executes a single Transaction against a Storage component with unique id `storage_id`
  """
  @spec tx_execute(%Transaction{}, atom()) :: no_return()
  def tx_execute(tx, storage_id) do
    message = Transaction.condensed(tx)
    # TODO: this message send / execution RPC has to be executed without delays to mimic a
    # transaction execution thread that executes all transactions from the global ordering
    # in sequential order
    send(storage_id, message)

    IO.puts("[node #{whoami()}] sent tx message #{inspect(message)} to be execute by Storage component #{storage_id}")
  end

  @doc """
  Attempts to execute a global ordering of Transactions in `global_orders` for the current `processing_epoch`
  against the Storage component residing on the same `physical` machine as the Scheduler RSM. If the
  global transaction ordering has been completed and is available to be executed, execute Transactions
  against the Storage component, increment the current `processing_epoch` to move up to the next epoch
  to process, and return the updated state, otherwise return the Scheduler state unchanged since the 
  global ordering is not done and we are waiting for partial ordering batches from Sequencer components
  """
  @spec attempt_tx_execute(%Scheduler{}) :: %Scheduler{}
  def attempt_tx_execute(state) do
    # first check if the global ordering for this `processing_epoch` epoch has been
    # completed yet by the `attempt_tx_interleave()` function
    if Map.has_key?(state.global_orders, state.processing_epoch) == false do
      # return the state unchanged 
      state
    else
      # retrieve the global ordering of Transactions for this `processing_epoch`
      ordered_txs = Map.get(state.global_orders, state.processing_epoch)

      # get the id for the Storage component that is on the same `physical` node
      # as the Scheduler 
      storage_id = Component.storage_id(state)

      # iterate the Transactions and execute against the Storage component
      # TODO: add functionality to handle deterministic tx failures
      Enum.map(ordered_txs, 
        fn tx ->
          tx_execute(_transaction=tx, _storage=storage_id)
        end
      )

      # once all of the Transactions have been executed, move up to the next epoch
      # to process by this Scheduler RSM and return the updated state
      increment_processing_epoch(state)
    end
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

        # save the batch of Transaction requests just received from the Sequencer
        # these do not necessarily have to be for the same epoch as `processing_epoch`
        # for this Scheduler
        state = save_received_batch(state, msg)

        # attempt to interleave the batches of Transactions for the current `processing_epoch`
        # that the Scheduler is currently trying to execute
        state = attempt_tx_interleave(state)

        # attempt to execute the Transactions for the current `processing_epoch`
        state = attempt_tx_execute(state)

        # continue receiving BatchTransactionMessage
        receive_batched_transaction_messages(state)
    end
  end

  @doc """
  Increment the processing epoch that the Scheduler component will attempt to finalize the 
  batch for and execute against the Storage component
  """
  @spec increment_processing_epoch(%Scheduler{}) :: %Scheduler{}
  def increment_processing_epoch(state) do
    updated_processing_epoch = state.processing_epoch + 1    
    %{state | processing_epoch: updated_processing_epoch}
  end

  @doc """
  Starts the Scheduler component RSM
  """
  @spec start(%Scheduler{}) :: no_return()
  def start(initial_state) do
    # TODO: do any initializations here for this component / process
    state = increment_processing_epoch(initial_state)

    # start accepting BatchTransactionMessage messages from the Sequencer components
    receive_batched_transaction_messages(state)
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
  @spec id(%Storage{} | %Sequencer{} | %Scheduler{}) :: atom()
  def id(proc) do
    replica = to_charlist(proc.replica)
    partition = to_charlist(proc.partition)
    type = to_charlist(proc.type)

    List.to_atom(replica ++ partition ++ '-' ++ type)
  end

  @doc """
  Returns replica + partition for this process which uniquely identifies a 
  `physical` node that this process belongs to in the Calvin system
  """
  @spec physical_node_id(%Storage{} | %Sequencer{} | %Scheduler{}) :: atom()
  def physical_node_id(proc) do
    replica = to_charlist(proc.replica)
    partition = to_charlist(proc.partition)

    List.to_atom(replica ++ partition)
  end

  @doc """
  Given a Sequencer or Scheduler component, returns the corresponding Storage
  component's unique ID which resides on the same `physical` node in the system
  """
  @spec storage_id(%Sequencer{} | %Scheduler{}) :: atom()
  def storage_id(proc) do
    node_id = physical_node_id(proc)
    List.to_atom(to_charlist(node_id) ++ '-' ++ to_charlist(:storage))
  end

  @doc """
  Given a Storage or Scheduler component, returns the corresponding Sequencer
  component's unique ID which resides on the same `physical` node in the system
  """
  @spec sequencer_id(%Storage{} | %Scheduler{}) :: atom()
  def sequencer_id(proc) do
    node_id = physical_node_id(proc)
    List.to_atom(to_charlist(node_id) ++ '-' ++ to_charlist(:sequencer))
  end

  @doc """
  Given a Storage or Sequencer component, returns the corresponding Scheduler
  component's unique ID which resides on the same `physical` node in the system
  """
  @spec scheduler_id(%Storage{} | %Sequencer{}) :: atom()
  def scheduler_id(proc) do
    node_id = physical_node_id(proc)
    List.to_atom(to_charlist(node_id) ++ '-' ++ to_charlist(:scheduler))
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

  def send_update_tx(client, key, val) do
    node = client.calvin_node
    tx = Transaction.update(key, val)

    send(node, tx)
  end

  def send_delete_tx(client, key) do
    node = client.calvin_node
    tx = Transaction.delete(key)

    send(node, tx)
  end
end
