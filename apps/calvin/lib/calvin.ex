# Calvin deployment module handling launch of Storage, Sequencer, and
# Scheduler components given a Configuration

defmodule Calvin do
  import Emulation, only: [spawn: 2, send: 2]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @doc """
  Given a Calvin configuration and a storage state, sets the key-value
  store states for all Storage components in the system to the given
  storage state map

  TODO: partition the given `storage_state` such that every Storage
  component only contains parts of the key-value store state that it
  is is responsible for based on the partition scheme
  """
  @spec set_storage(%Configuration{}, %{}) :: no_return()
  def set_storage(configuration, storage_state) do
    Enum.map(Configuration.get_replica_view(configuration), 
      fn replica ->
        storage_view = Configuration.get_storage_view(configuration, replica)
        Enum.map(storage_view, 
          fn storage_id ->
            send(storage_id, {:set_kv_store, storage_state})
          end
        )
      end
    )
  end

  # Helper function to launch `num_partitions` Sequencer components
  # for a given replica with a given Configuration and partition view
  defp launch_partitioned_sequencer(replica, configuration) do
    # create as many Sequencer components as there are partitions
    partition_view = Configuration.get_partition_view(configuration)
    Enum.map(partition_view, 
      fn partition -> 
        sequencer_proc_partitioned = Sequencer.new(_replica=replica, _partition=partition, configuration)
        sequencer_proc_id = Component.id(sequencer_proc_partitioned)

        # spawn each Sequencer
        spawn(sequencer_proc_id, fn -> Sequencer.start(sequencer_proc_partitioned) end)
      end
    )
  end

  # Helper function to launch `num_partitions` Scheduler components
  # for a given replica with a given Configuration and partition view
  defp launch_partitioned_scheduler(replica, configuration) do
    # create as many Scheduler components as there are partitions
    partition_view = Configuration.get_partition_view(configuration)
    Enum.map(partition_view, 
      fn partition -> 
        scheduler_proc_partitioned = Scheduler.new(_replica=replica, _partition=partition, configuration)
        scheduler_proc_id = Component.id(scheduler_proc_partitioned)

        # spawn each Scheduler
        spawn(scheduler_proc_id, fn -> Scheduler.start(scheduler_proc_partitioned) end)
      end
    )
  end

  # Helper function to launch `num_partitions` Storage components
  # for a given replica with a given Configuration and partition view
  defp launch_partitioned_storage(replica, configuration) do
    # create as many Storage components as there are partitions
    partition_view = Configuration.get_partition_view(configuration)
    Enum.map(partition_view, 
      fn partition -> 
        storage_proc_partitioned = Storage.new(_replica=replica, _partition=partition)
        storage_proc_id = Component.id(storage_proc_partitioned)

        # spawn each Storage
        spawn(storage_proc_id, fn -> Storage.start(storage_proc_partitioned) end)
      end
    )
  end

  @doc """
  Launches a partitioned, replicated Calvin system given a Configuration with `num_partitions`
  and `num_replicas` given by `configuration.partition_scheme.num_partitions` and 
  `configuration.replication_scheme.num_replicas`
  """
  @spec launch(%Configuration{}) :: no_return()
  def launch(configuration) do
    num_partitions = configuration.partition_scheme.num_partitions
    num_replicas = configuration.replication_scheme.num_replicas

    # for each replica launch a partitioned Sequencer, Scheduler, and Storage
    # component
    replica_view = Configuration.get_replica_view(configuration)
    Enum.map(replica_view, 
      fn replica ->
        # launch Sequencer components
        launch_partitioned_sequencer(_replica=replica, _configuration=configuration)
        # launch Scheduler components
        launch_partitioned_scheduler(_replica=replica, _configuration=configuration)
        # launch Storage components
        launch_partitioned_storage(_replica=replica, _configuration=configuration)
      end
    )

    IO.puts("Launched a Calvin deployment of #{num_replicas} replica(s), each partitioned across #{num_partitions} machine(s) per replica")
  end
end

# Storage component process for the Calvin system. This is an RSM that 
# keeps a key-value store in memory and can write to disk or 
# take snapshots to persist data, but overall we assume this component
# mimics any CRUD interface that can be substituted in.

defmodule Storage do
  import Emulation, only: [send: 2, whoami: 0, mark_unfuzzable: 0]
  import Kernel, except: [send: 2]

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
  @spec exec_storage_command(%Storage{}, atom(), any(), any()) :: {any(), any() | :ok | :notok, %Storage{}}
  def exec_storage_command(state, command, key, val \\ nil) do
    case command do
      :READ ->
        # if the command is READ, return the value that is the result of the
        # read from the KV store and return the state which doesn't have to be
        # updated in this case

        read_value = Map.get(state.store, key)
        {key, read_value, state}

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
          {key, :ok, updated_state}
        end

      :UPDATE ->
        # if the command is UPDATE, if the key already exists, then update
        # the KV store and return :ok, otherwise do not change anything and
        # return :notok

        if Map.has_key?(state.store, key) do
          updated_store = Map.put(state.store, key, val)
          updated_state = %{state | store: updated_store}
          {key, :ok, updated_state}
        else
          # indicate an error
          {key, :notok, state}
        end
        
      :DELETE ->
        # if the command is DELETE, remove the value associated with key from the KV 
        # store and return the updated state

        updated_store = Map.delete(state.store, key)
        updated_state = %{state | store: updated_store}
        {key, :ok, updated_state}

    end
  end

  @doc """
  Run the Storage component RSM, listen for storage execution commands from a Scheduler, and execute 
  operations on the key-value store in memory
  """
  @spec receive_commands(%Storage{}) :: no_return()
  def receive_commands(state) do
    Debug.log("current state of KV store: #{inspect(state.store)}")   

    receive do
      # raw CRUD messages for the key-value store
      {scheduler_sender, {:READ, key}} ->
        Debug.log("received a READ request for key {#{key}}")

        {ret_key, ret_val, state} = exec_storage_command(state, :READ, key)

        # send back the response to the Scheduler that requested the READ 
        send(scheduler_sender, {ret_key, ret_val})

        receive_commands(state)
      {_scheduler_sender, {:CREATE, key, val}} ->
        Debug.log("received a CREATE request setting key {#{key}} to value {#{val}}")

        {_ret_key, _ret_val, state}= exec_storage_command(state, :CREATE, key, val)

        receive_commands(state)
      {_scheduler_sender, {:UPDATE, key, val}} -> 
        Debug.log("received a UPDATE request updating key {#{key}} to value {#{val}}")
        
        {_ret_key, _ret_val, state} = exec_storage_command(state, :UPDATE, key, val)

        receive_commands(state)
      {_scheduler_sender, {:DELETE, key}} ->
        Debug.log("received a DELETE request deleting value associated with key {#{key}}")

        {_ret_key, _ret_val, state} = exec_storage_command(state, :DELETE, key)
        
        receive_commands(state)

      # ----------------------------
      # testing / debugging messages
      # ----------------------------

      {debug_sender, :get_kv_store} ->
        send(debug_sender, state.store)
        receive_commands(state)

      {_debug_sender, {:set_kv_store, store_state}} ->
        # update the Storage store state to the provided key value store
        state = %{state | store: store_state}
        receive_commands(state)
      end
  end

  @doc """
  Starts the Storage component RSM
  """
  @spec start(%Storage{}) :: no_return()
  def start(initial_state) do
    # mark as unfuzzable since every `send` message to the Storage RSM
    # will be sent by a Scheduler on the same physical machine
    mark_unfuzzable()

    # start accepting storage execution commands
    receive_commands(initial_state)
  end
end

# Sequencer component process for the Calvin system. This is an RSM that 
# receives requests from clients during individual epochs, appends them locally
# to a log-like structure, and when the epoch is completed, replicates the inputs
# before forwarding them to the Scheduler layer for execution

defmodule Sequencer do
  import Emulation, only: [send: 2, timer: 1, whoami: 0, timer: 2]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  defstruct(
    type: :sequencer,
    replica: nil,
    partition: nil,

    # deployment Configuration
    configuration: nil,

    epoch_timer_duration: 2000, # default epoch duration is 10ms
    epoch_timer: nil,

    current_epoch: nil,
    # local logs of entries for epochs
    # this is a map of {epoch number => log of requests for that epoch}
    epoch_logs: %{},

    # Raft protocol state for when using Raft-based synchronous
    # replication of Transaction batches
    raft: nil
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
  Sends out BatchTransactionMessage messages in broadcast manner to all Schedulers on every
  partition within the same replica as the current Sequencer process / component. Each message
  contains only the batch of Transactions that the recipient Scheduler needs to participate in, 
  which is decided by the PartitionScheme as part of the current Configuration
  """
  @spec broadcast_batch(%Sequencer{}, non_neg_integer(), [%Transaction{}]) :: %Sequencer{}
  def broadcast_batch(state, epoch, batch) do
    # prepare for broadcast by generating the active/passive participating
    # partitions for all Transactions in the given batch
    batch = PartitionScheme.generate_participating_partitions(
      _tx_batch=batch,
      _partition_scheme=state.configuration.partition_scheme
    )

    # split the log of Transactions for the current epoch by partitioning based on the
    # current PartitionScheme
    partitioned_tx_batches = PartitionScheme.partition_transactions(
      _tx_batch=batch
    )
    Debug.log("partitioned batch for epoch #{epoch}: #{inspect(partitioned_tx_batches)}")

    # get all of the partition numbers in a replica via the current Configuration
    partition_view = Configuration.get_partition_view(state.configuration)
    Debug.log("list of partitions per replica: #{inspect(partition_view)}")

    # send out BatchTransactionMessages to all Schedulers within the same replica as the
    # current Sequencer process / component
    Enum.map(partition_view, 
      fn partition ->
        # construct a BatchTransactionMessage with the Transactions in the partitioned batch
        # that need to be sent to this particular partition within the replica
        batch_tx_msg = %BatchTransactionMessage{
          sequencer_id: whoami(),
          epoch: epoch,
          # log of Transactions for this particular partition or empty log
          # if no Transactions were acquired that need to be sent to this
          # partition
          batch: Map.get(partitioned_tx_batches, partition, [])
        }

        # construct a unique id for a recipient Scheduler component within the current replica
        # given a partition
        scheduler_id = Component.id(_replica=state.replica, _partition=partition, _type=:scheduler)
        
        # send a BatchTransactionMessage to this specific Scheduler component
        send(scheduler_id, batch_tx_msg)

        Debug.log("sent BatchTransactionMessage #{inspect(batch_tx_msg)} to scheduler process #{scheduler_id}")
      end
    )

    # return the updated state
    %{state | epoch_logs: Map.put(state.epoch_logs, epoch, batch)}
  end

  @doc """
  Forwards a Transaction request to a randomly chosen Sequencer component on the main replica
  if using ReplicationScheme.Async or on the leader replica if using ReplicationScheme.Raft
  """
  @spec forward_request(%Sequencer{}, %Transaction{}) :: no_return()
  def forward_request(state, tx) do
    # forward the request to either the main replica if using `async` replication
    # or to the leader replica if using `raft` replication
    replication_scheme = state.configuration.replication_scheme

    recipient_replica = if Configuration.using_replication?(state.configuration) == :async, 
      do: replication_scheme.main_replica,
      else: ReplicationScheme.Raft.get_leader_for_partition(
        _replication_scheme=replication_scheme,
        _paritition=state.partition
      )

    # get a list of Sequencers on the recipient replica that we can forward this Transaction to
    sequencers = Configuration.get_sequencer_view(state.configuration, _replica=recipient_replica)

    # pick a random Sequencer to forward the Transaction to
    sequencer = Enum.random(sequencers)
    send(sequencer, tx)

    Debug.log("forwarded tx #{inspect(tx)} to sequencer #{sequencer}")
  end

  @doc """
  Handles an incoming Transaction request with ReplicationScheme.Async
  replication mode
  """
  @spec handle_tx_with_async_replication(%Sequencer{}, %Transaction{}) :: %Sequencer{}
  def handle_tx_with_async_replication(state, tx) do
    # if the Sequencer RSM is not located on the main replica, forward this
    # Transaction request to any of the Sequencers on the main replica given by
    # the Configuration
    if Component.on_main_replica?(state) == false do
      # forward the Transaction
      forward_request(state, tx)

      # return the state unchanged
      state
    else
      # timestamp this Transaction at time of receiving
      tx = Transaction.add_timestamp(tx)
      Debug.log("tx updated to #{inspect(tx)}")

      # add the incoming Transaction to the Sequencer's local log
      state = add_to_log(state, tx)
      Debug.log("local log for epoch #{state.current_epoch} updated to #{inspect(Map.get(state.epoch_logs, state.current_epoch))}")

      # return the updated state
      state
    end
  end

  @doc """
  Given a state for the Raft protocol, updates the local Sequencer RSM 
  state of the given Sequencer process to hold the updated Raft state
  """
  @spec update(%Raft{}, %Sequencer{}) :: %Sequencer{}
  def update(raft_state, state) do
    %{state | raft: raft_state}
  end

  @doc """
  Initializes the state of the Raft protocol for the given Sequencer RSM
  """
  @spec initialize_raft(%Sequencer{}) :: %Sequencer{}
  def initialize_raft(state) do
    # get a Raft state based on the current Sequencer state
    raft = Raft.init(_sequencer_state=state)

    case raft.current_role do
      :leader ->
        Raft.Leader.make(raft) |> update(state)

      :follower ->
        Raft.Follower.make(raft) |> update(state)
    end
  end

  @doc """
  Handles an incoming Transaction request with ReplicationScheme.Raft
  replication mode

  TODO: add support to forward the Transactions to the leader replica at a time interval
  instead of immediately
  """
  @spec handle_tx_with_raft_replication(%Sequencer{}, %Transaction{}) :: %Sequencer{}
  def handle_tx_with_raft_replication(state, tx) do
    # if the Sequencer RSM is not located on the Raft leader replica, forward this
    # Transaction request to any of the Sequencers on the leader replica given by
    # the Configuration
    if Component.on_leader_replica?(state) == false do
      # forward the Transaction
      forward_request(state, tx)

      # return the state unchanged
      state
    else
      # timestamp this Transaction at time of receiving
      tx = Transaction.add_timestamp(tx)
      Debug.log("tx updated to #{inspect(tx)}")

      # add the incoming Transaction to the Sequencer's local log
      state = add_to_log(state, tx)
      Debug.log("local log for epoch #{state.current_epoch} updated to #{inspect(Map.get(state.epoch_logs, state.current_epoch))}")

      # return the updated state
      state
    end
  end

  @doc """
  Run the Sequencer component RSM, listen for client requests, append them to log and keep track of
  current epoch timer. Based on the current replication scheme, also send and receive messages specific
  to the replication scheme
  """
  @spec receive_requests(%Sequencer{}) :: no_return()
  def receive_requests(state) do
    receive do
      # Transaction request from a client
      {client_sender, tx = %Transaction{
        operations: _operations
      }} ->
        Debug.log("received a Transaction request: #{inspect(tx)} from client {#{client_sender}}")
        
        # handle the request based on the replication mode
        case Configuration.using_replication?(state.configuration) do
          :async ->
            state = handle_tx_with_async_replication(state, _transaction=tx)

            # continue listening for requests
            receive_requests(state)
          :raft ->
            state = handle_tx_with_raft_replication(state, _transaction=tx)

            # continue listening for requests
            receive_requests(state)
        end

      {client_sender, :ping} ->
        Debug.log("received a ping request from client {#{client_sender}}")

        # continue listening for requests
        receive_requests(state)

      # Async.ReplicateBatch request sent by the Sequencers on the main replica which notifies the Sequencers 
      # on the secondary replicas of Transactions received during the given epoch and allows the
      # Sequencer RSM to update it's state and move up the epoch to synchronize
      {sequencer_sender, %Async.ReplicateBatch{
        epoch: epoch,
        batch: batch
      }} ->
        Debug.log("received an Async.ReplicateBatch request from Sequencer #{sequencer_sender}
        for epoch: #{epoch}
        batch: #{inspect(batch)}")

        # set the current epoch to the one that the main replica has sent via the 
        # Async.ReplicateBatch request in order to sync up and set the log to be
        # empty initially
        state = %{state | current_epoch: epoch}
        state = initialize_log(state, state.current_epoch)
        
        # append all of the Transactions received from the main replica to the
        # local log using a reduction to continiously update the state of the
        # Sequencer RSM as we are adding Transaction entries with `add_to_log` 
        state = Enum.reduce(batch, state, fn tx, acc -> add_to_log(acc, tx) end)

        Debug.log("log state after reduction: #{inspect(Map.get(state.epoch_logs, state.current_epoch))}")

        # now that the log for the epoch is synced up with the Sequencer on the main replica
        # within this Sequencer's replica group, send out a BatchTransactionMessage to all of
        # the Schedulers within the same replica as the current Sequencer
        state = broadcast_batch(state, 
          _for_epoch=state.current_epoch,
          # all of the Transactions for the current epoch acquired by this Sequencer
          _batch=Map.get(state.epoch_logs, state.current_epoch)
        )

        # continue listening for requests
        receive_requests(state)

      # epoch timer message signifying the end of the current epoch
      # with async replication the epoch timer is active on Sequencers on the 
      # main replica and with synchronous Raft-based replication the epoch timer
      # is active on Sequencers on the leader replica
      :epoch_timer ->
        Debug.log("epoch #{state.current_epoch} has ended, sending BatchTransactionMessage to Schedulers, then starting new epoch")

        # handle the end of current epoch based on the current replication scheme. If
        # using ReplicationScheme.Async, replicate to other secondary replica Sequencers
        # and send the batch for this epoch to the Schedulers immediately, otherwise, if
        # using ReplicationScheme.Raft, start the replication and only update the current 
        # epoch, as the Sequencer can only reliably consider a batch committed, and hence 
        # apply the entry at the current log position, once it knows the entry is committed.
        # Applying the entry is in this case equivalent to sending the entry to the Scheduler
        # processes

        case Configuration.using_replication?(state.configuration) do
          :async ->
            # asynchronously replicate the batch for the current epoch to other Sequencers 
            # in the current Sequencer's replication group
            replicate_batch_async(state)

            # since using async replication, now can broadcast the relevant batches (those
            # Transactions in which the partition will need to participate in) for each
            # Scheduler within the same replica as the current Sequencer
            state = broadcast_batch(state, 
              _for_epoch=state.current_epoch,
              # all of the Transactions for the current epoch acquired by this Sequencer
              _batch=Map.get(state.epoch_logs, state.current_epoch)
            )

            # increment the epoch from current -> current + 1
            # and, if necessary, restart the epoch timer
            state = increment_epoch(state)

            # continue listening for requests
            receive_requests(state)

          :raft ->
            # start to synchronously replicate the batch for the current epoch to other
            # Sequencers in the current Sequencer's replication group with Raft. Cannot yet
            # broadcast the batch to the Schedulers since need to commit the batch first
            # by replicating to enough members of the replication group
            state = replicate_batch_raft(state)

            # increment the epoch from current -> current + 1
            state = increment_epoch(state)

            # continue listening for requests
            receive_requests(state)
        end

      # ------------------------------
      # messages for the Raft protocol
      # ------------------------------

      {sequencer_sender, rpc = %Raft.AppendEntries{}} ->
        
        # handle the AppendEntries RPC based on the current role of the Sequencer's Raft 
        # state. If the current role is the `leader`, potentially update state to become 
        # a follower. If the current role is `follower`, attempt to update the local Raft
        # log with the entries that the leader is attempting to replicate and respond with
        # an AppendEntries.Response message

        case Raft.current_role?(state.raft) do
          :leader ->
            # handle the AppendEntries request msg by updating necessary leader state
            state = Raft.Leader.Handler.append_entries_request(state.raft, rpc, sequencer_sender) |> update(state)

            receive_requests(state)

          :follower ->
            # handle the AppendEntries request msg by updating necessary follower state
            state = Raft.Follower.Handler.append_entries_request(state.raft, rpc, sequencer_sender) |> update(state)

            # check if can apply the next entry in the Raft log locally, since perhaps the
            # Sequencer has received confirmation from the leader Sequencer of the current
            # replication group marking the next Raft log entry as commited
            if Raft.Follower.can_apply_next?(state.raft) do
              # update the Raft state prior to applying
              state = Raft.Follower.prepate_to_apply(state.raft) |> update(state)

              Debug.log("[node #{whoami()}][follower] ready to apply at Raft log index: #{inspect(state.raft.last_applied)}", _role="follower")
              
              log_entry_to_apply = Raft.Log.get_entry_at_index(state.raft.log, _apply_index=state.raft.last_applied)

              Debug.log("applying a LogEntry batch of transactions: #{inspect(log_entry_to_apply.batch)}", _role="follower")
              Debug.log("the apply is for epoch [#{log_entry_to_apply.index}]", _role="follower")

              # apply the entry at the Raft log to be applied at by sending the batch of Transactions
              # for that log entry to the Schedulers on the same replica as the current Sequencer 
              # process that is in Raft `follower` state
              state = broadcast_batch(state, 
                _for_epoch=log_entry_to_apply.index, 
                _batch=log_entry_to_apply.batch
              )

              # continue listening for requests
              receive_requests(state)
            else
              # no update yet to the commit index from the Sequencer that is the leader of
              # the current replication group, so continue listening for requests
              receive_requests(state)
            end
        end

      {sequencer_sender, rpc = %Raft.AppendEntries.Response{}} ->

        # for the AppendEntries response, if the current Raft state is the `leader`, attempt
        # to commit the Raft log entry at the next index, otherwise if in `follower` state,
        # ignore the message since it is the leader's responsibility to commit entries

        case Raft.current_role?(state.raft) do
          :leader ->
            # handle the AppendEntries response msg by updating necessary leader state
            state = Raft.Leader.Handler.append_entries_response(state.raft, rpc, sequencer_sender) |> update(state)

            # check if can commit the next entry in the Raft log, which would mean 
            # finally sending the batch of Transactions to all Schedulers on the same
            # replica as the current Sequencer process
            if Raft.Leader.can_commit_next?(state.raft) do
              # update the Raft state prior to commiting
              state = Raft.Leader.prepate_to_commit(state.raft) |> update(state)

              Debug.log("ready to commit at Raft log index #{state.raft.last_applied}", _role="leader")
              log_entry_to_commit = Raft.Log.get_entry_at_index(state.raft.log, _commit_index=state.raft.last_applied)

              Debug.log("commiting a LogEntry batch of transactions: #{inspect(log_entry_to_commit.batch)}", _role="leader")
              Debug.log("the commit is for epoch [#{log_entry_to_commit.index}]", _role="leader")
              
              # since the Transaction batch at the index in the Raft log that is equivalent
              # to the epoch has been marked as safe to commit, perform the commit by
              # sending the batch in broadcast manner to all Schedulers on this replica
              state = broadcast_batch(state, 
                _for_epoch=log_entry_to_commit.index, 
                _batch=log_entry_to_commit.batch
              )

              # update followers of the replica group since the commit index was updated earlier
              # by the AppendEntries response msg handler
              Raft.Leader.broadcast_heartbeat_rpc(state.raft)

              # continue listening for requests
              receive_requests(state)
            else
              # keep waiting for more AppendEntries.Response since have not yet heard
              # from enough of the members of the replication group to safely mark the
              # next log entry as commited
              receive_requests(state)
            end

          :follower ->
            receive_requests(state)
        end

      # ----------------------------
      # testing / debugging messages
      # ----------------------------

      {debug_sender, :get_state} ->
        send(debug_sender, state)
        receive_requests(state)
    end
  end

  @doc """
  Syncronously starts to replicate the batch of Transactions received on the current Sequencer
  RSM, which resides on the leader replica, to all of the Sequencer components on other replicas
  that are in this Sequencer's replication group via Raft
  """
  @spec replicate_batch_raft(%Sequencer{}) :: %Sequencer{}
  def replicate_batch_raft(state) do
    # perform the steps before replication
    state = Raft.prepare_to_replicate(state.raft, _batch=Map.get(state.epoch_logs, state.current_epoch)) |> update(state)

    # replicate the batch via AppendEntries RPC from the Raft protocol
    state = Raft.replicate(state.raft) |> update(state)

    state
  end

  @doc """
  Asynchronously replicates the batch of Transactions received on the current Sequencer RSM,
  which resides on the main replica, to all of the Sequencer components on other replicas that 
  are in this Sequencer's replication group
  """
  @spec replicate_batch_async(%Sequencer{}) :: no_return()
  def replicate_batch_async(state) do
    # create the async replication request message
    async_replicate_msg = %Async.ReplicateBatch{
      epoch: state.current_epoch,
      # log of Transactions for the current epoch
      batch: Map.get(state.epoch_logs, state.current_epoch)
    }
    # get the unique ids of all other replicas other than the current replica, since
    # those comprise the replication group to which the current main replica has to
    # replicate the batch to
    replication_group_view = ReplicationScheme.get_all_other_replicas(state, state.configuration.replication_scheme)
    
    # send requests to all replicas within this replication group of the partition of 
    # the current Sequencer component
    Enum.map(replication_group_view, 
      fn replica ->
        # construct a unique id for a recipient Sequencer component within 
        # the replication group
        sequencer_id = Component.id(_replica=replica, _partition=state.partition, _type=:sequencer)
        
        # send the message to the replica Sequencer
        send(sequencer_id, async_replicate_msg)
      end
    )
  end

  @doc """
  Starts the epoch timer of the Sequencer RSM if the Sequencer is either part of the main
  replica when using ReplicationScheme.Async or part of the leader replica when using 
  ReplicationScheme.Raft. In the current configuration, the leader or the main replica is
  responsible for maintaining a timer to increment epochs and initiate either async or 
  synchronous Raft-based replication of Transactional input for that epoch
  """
  @spec start_epoch_timer(%Sequencer{}) :: %Sequencer{}
  def start_epoch_timer(state) do
    case Configuration.using_replication?(state.configuration) do
      :async ->
        if Component.on_main_replica?(state) do
          # start the timer if using async replication and the current Sequencer
          # is on the main replica, since it will be in charge of relaying
          # the epoch number to the secondary replicas
          new_epoch_timer = timer(state.epoch_timer_duration, :epoch_timer)
          %{state | epoch_timer: new_epoch_timer}
        else
          state
        end
      :raft ->
        if Component.on_leader_replica?(state) do
          # start the timer if using synchronous Raft-based replication and the current
          # sequencer is on the leader replica, since it will be in charge of initiating
          # the replication of collected Transactions
          new_epoch_timer = timer(state.epoch_timer_duration, :epoch_timer)
          %{state | epoch_timer: new_epoch_timer}
        else
          state
        end
    end
  end

  @doc """
  Initializes the local log for a given epoch on the Sequencer RSM 
  """
  @spec initialize_log(%Sequencer{}, non_neg_integer()) :: %Sequencer{}
  def initialize_log(state, epoch) do
    updated_epoch_logs = Map.put(state.epoch_logs, epoch, [])
    %{state | epoch_logs: updated_epoch_logs}
  end

  @doc """
  Updates the Sequencer RSM to increment the current epoch by 1 and create a new empty log
  for that epoch to hold client requests. If the Sequencer is part of the main replica, start
  the timer for a duration of an epoch, otherwise don't since the epoch in secondary replicas
  will be incremented by incoming ReplicateBatchRequest messages from Sequencers that are part
  of the main replica
  """
  @spec increment_epoch(%Sequencer{}) :: %Sequencer{}
  def increment_epoch(state) do
    # increment the epoch in the RSM
    old_epoch = state.current_epoch
    state = %{state | current_epoch: old_epoch + 1}
    Debug.log("-------------------- EPOCH #{state.current_epoch} --------------------")
    Debug.log("incremented epoch from #{old_epoch} -> #{state.current_epoch}")

    # create an empty log for the current updated epoch in the RSM
    state = initialize_log(state, state.current_epoch)

    Debug.log("current state of `epoch_logs`: #{inspect(state.epoch_logs)}")

    # if on main replica or leader replica, start the timer for
    # duration of epoch
    state = start_epoch_timer(state)

    state
  end

  @doc """
  Starts the Sequencer component RSM
  """
  @spec start(%Sequencer{}) :: no_return()
  def start(initial_state) do
    # configure the initial Raft state for the Sequencer process if using Raft-based
    # synchronous replication with ReplicationScheme.Raft
    if Configuration.using_replication?(initial_state.configuration) == :raft do
      # initialize the state for Raft protocol
      state = initialize_raft(initial_state)

      # increment the epoch from 0 -> 1
      state = increment_epoch(state)

      # start accepting requests from clients
      receive_requests(state)
    else
      # increment the epoch from 0 -> 1
      state = increment_epoch(initial_state)

      # start accepting requests from clients
      receive_requests(state)
    end
  end
end

# Scheduler component process for the Calvin system. This is an RSM that 
# receives batched requests via a message from the Sequencer components, and
# is then responsible for interleaving batches from different partitions from the
# same replica if necessary, and executing the transactions in the final total order
# against the Storage component. The Scheduler 'schedules' tx execution in a deterministic
# manner in the order that it determines the transactions to be in for a given epoch

defmodule Scheduler do
  import Emulation, only: [whoami: 0, send: 2, mark_unfuzzable: 0]
  import Kernel, except: [send: 2]

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

    Debug.log("partial tx orders (epoch #{epoch}): 
    \n#{inspect(state.partial_orders)}\n")

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

    Debug.log("all txs for epoch #{epoch} flattened: #{inspect(all_txs)}")
    
    # sort all of the Transactions according to their timestamps
    sorted_txs = Enum.sort(all_txs, 
      fn tx1, tx2 ->
        case DateTime.compare(tx1.timestamp, tx2.timestamp) do
          :lt ->
            true
          _ -> 
            false
        end
      end
    )

    Debug.log("sorted global tx order: #{inspect(sorted_txs)}")
    
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
    expected = state.configuration.partition_scheme.num_partitions
    received = length(Map.get(state.partial_orders, state.processing_epoch, []))

    Debug.log("attempting to interleave txs for epoch #{state.processing_epoch}
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
  Recursively collects local reads one by one until all expected local reads are collected
  from the local Storage component
  """
  @spec perform_local_reads(%{}, atom(), integer(), integer()) :: %{}
  def perform_local_reads(read_results, storage_id, collected, expected) do
    # collect the read request results from the Storage component
    receive do
      {^storage_id, msg = {read_key, read_val}} -> 
        IO.puts("received response: #{inspect(msg)} from #{storage_id}")

        collected = collected + 1
        read_results = Map.put(read_results, read_key, read_val)

        if collected == expected do
          # return since collected all of the responses
          read_results
        else
          # keep waiting for more responses 
          perform_local_reads(
            read_results,
            storage_id,
            collected,
            expected
          )
        end
    end
  end

  @doc """
  Performs local reads by looking up the values of all records in the read set
  that are stored locally on the current Sequencer's partition and returns a map
  of {key -> value} of collected local reads
  """
  @spec perform_local_reads(%Sequencer{}, %{}, atom()) :: %{}
  def perform_local_reads(state, read_set, storage_id) do
    IO.puts("performing local reads...")

    # computes how many local reads are expected by iterating values in the Transactions
    # read set and counting how many of the values are stored locally on the current partition
    expected = Enum.reduce(read_set, 0, 
      fn val, acc ->
        if PartitionScheme.is_local?(val, state.partition, state.configuration.partition_scheme) do
          # send a request for a read to the Storage component
          read_request = {:READ, val}
          IO.puts("sending a read request #{inspect(read_request)} to #{storage_id}")

          send(storage_id, read_request)

          acc + 1
        else
          acc
        end
      end
    )

    IO.puts("expecting read results: #{expected}")
    read_results = %{}

    if expected == 0 do
      IO.puts("not expecting any reads, returning empty local read set")
      read_results
    else
      IO.puts("waiting for responses from Storage")
      # recursively collect all of the expected local reads from the local Storage component
      read_results = perform_local_reads(read_results, storage_id, _collected=0, _expected=expected)

      IO.puts("returning read_results: #{inspect(read_results)}")
      read_results
    end
  end

  @doc """
  Serves remote reads collected from previous step 2 by forwarding the read results to all
  actively participating Schedulers. Since passively participating Schedulers do not need
  to perform any modifications to the data, they do not need to collect any of the local reads
  results from other Schedulers, therefore those Schedulers are finished with transaction 
  execution after this phase
  """
  @spec serve_local_reads(%Sequencer{}, [non_neg_integer()], %{}) :: no_return()
  def serve_local_reads(state, active_participants, local_reads) do
    # filter out the current Scheduler's partition if needed
    active_participants = active_participants |> Enum.filter(fn partition -> partition != state.partition end)

    Enum.map(active_participants, 
      fn partition ->
        # construct a unique id for a recipient Scheduler component within the current replica
        # given a partition
        scheduler_id = Component.id(_replica=state.replica, _partition=partition, _type=:scheduler)
        
        # construct a LocalReadsTransactionMessage to send the local reads from the
        # current Scheduler to the other actively participating Scheduler
        local_reads_msg = %LocalReadsTransactionMessage{
          local_reads: local_reads
        }

        # send a LocalReadsTransactionMessage to this specific Scheduler component
        send(scheduler_id, local_reads_msg)
      end
    )
  end

  @doc """
  Given a partition and a set of actively participating partitions, returns whether the 
  given partition is part of those partitions that are actively participating in the 
  transaction execution
  """
  @spec is_actively_participating?(non_neg_integer(), [non_neg_integer()]) :: boolean()
  def is_actively_participating?(partition, active_participants) do
    if MapSet.member?(active_participants, partition) do
      true
    else
      false
    end
  end

  @doc """
  Recursively collects all remote reads one by one until all expected remote reads are collected
  from all other participating Schedulers
  """
  @spec collect_remote_reads(%{}, integer(), integer()) :: %{}
  def collect_remote_reads(remote_reads, collected, expected) do
    # collect the remote reads by waiting for LocalReadsTransactionMessages
    receive do
      {scheduler, %LocalReadsTransactionMessage{
        local_reads: local_reads
      }} -> 
        IO.puts("received a LocalReadsTransactionMessage #{inspect(local_reads)} from #{scheduler}")

        collected = collected + map_size(local_reads)
        remote_reads = Map.merge(remote_reads, local_reads)

        if collected == expected do
          # return since collected all of the LocalReadsTransactionMessages
          remote_reads
        else
          # keep waiting for more messages
          collect_remote_reads(
            remote_reads,
            collected,
            expected
          )
        end
    end
  end

  @doc """
  Collects remote reads by waiting for LocalReadsTransactionMessages from all other participating
  Schedulers to serve their corresponding local reads in step 3. Returns a map of {key -> value}
  of collected remote reads
  """
  @spec collect_remote_reads(%Sequencer{}, %{}) :: %{}
  def collect_remote_reads(state, read_set) do
    IO.puts("collecting remote reads...")
    
    # computes how many remote reads to expect by counting elements of the read set that
    # are not stored locally on the current partition
    expected = Enum.reduce(read_set, 0, 
      fn val, acc ->
        if PartitionScheme.is_local?(val, state.partition, state.configuration.partition_scheme) == false do
          acc + 1
        else
          acc
        end
      end
    )

    IO.puts("expecting remote reads: #{expected}")
    remote_reads = %{}

    if expected == 0 do
      IO.puts("not expecting any remote reads, returning")
      remote_reads
    else
      IO.puts("waiting for LocalReadsTransactionMessages")
      # recursively collect all of the remote reads via LocalReadsTransactionMessage
      remote_reads = collect_remote_reads(remote_reads, _collected=0, _expected=expected)

      IO.puts("returning remote_reads: #{inspect(remote_reads)}")
      remote_reads
    end
  end

  @doc """
  Finalizes the execution of a given Transaction against a Storage component after acquiring
  sets of both the local and remote required reads and applies all **local** write operations
  of the given Transaction, since non-local writes will be executed by other Schedulers
  on the appropriate partition at this stage of transaction execution 
  """
  @spec apply_local_writes(%Sequencer{}, %Transaction{}, %{}, %{}, atom()) :: no_return()
  def apply_local_writes(state, tx, local_reads, remote_reads, storage_id) do
    # go through the operations and apply any local writes
    Enum.map(tx.operations, 
      fn op ->
        # execute only local writes, as non-local writes will be seen as local by other
        # partitions and executed by Schedulers there
        if Transaction.Op.is_write?(op) && PartitionScheme.is_local?(op.key, state.partition, state.configuration.partition_scheme) do
          # evaluate the operation's expression if necessary using the local and remote 
          # reads collected 
          all_collected_reads = Map.merge(local_reads, remote_reads)
          IO.puts("all collected reads: #{inspect(all_collected_reads)}")

          op = Transaction.Op.evaluate_expr(op, all_collected_reads)

          IO.puts("evaluated operation #{inspect(op)}")

          # get the condensed version of the opeeration
          message = Transaction.Op.condensed(op)

          IO.puts("condensed version: #{inspect(message)}")
          
          # TODO: this message send / execution RPC has to be executed without delays to mimic a
          # transaction execution thread that executes all transactions from the global ordering
          # in sequential order
          send(storage_id, message)
          Debug.log("sent tx Operation message #{inspect(message)} to be executed by Storage component #{storage_id}")
        end
      end
    )
  end

  @doc """
  Executes a single Transaction against a Storage component with unique id `storage_id`
  """
  @spec tx_execute(%Scheduler{}, %Transaction{}, atom()) :: no_return()
  def tx_execute(state, tx, storage_id) do
    # mark the Transaction as finished since we are about to execute against
    # the Storage
    time = DateTime.utc_now()
    tx = Transaction.set_finished(tx, _time=time, _node=Component.physical_node_id(state))

    # step 1 - read/write set analysis, with read/write sets and participating partitions
    # already generated by the Sequencers when broadasting the batch
    read_set = tx.read_set
    active_participants = tx.active_participants

    # step 2 - perform local reads
    local_read_set = perform_local_reads(state, read_set, storage_id)
    IO.puts("local read set collected: #{inspect(local_read_set)}")

    # step 3 - serve remote reads
    serve_local_reads(state, 
      _active_participants=active_participants,
      _local_reads=local_read_set
    )

    # if the current Scheduler is only a passive participant, then it is done after
    # this step
    if is_actively_participating?(state.partition, active_participants) == false do
      IO.puts("[#{whoami()}] is a passive participant, done now")
    else
      IO.puts("[#{whoami()}] is an active participant now will wait for remote reads if needed")

      # step 4 - collect remote read results
      remote_reads = collect_remote_reads(state, read_set)
      
      IO.puts("now can finish transaction, 
      local reads: #{inspect(local_read_set)}
      remote reads: #{inspect(remote_reads)}")

      # step 5 - Transaction logic execution and applying writes
      apply_local_writes(state, tx, local_read_set, remote_reads, storage_id)
    end
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
          tx_execute(state, _transaction=tx, _storage=storage_id)
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
        # TODO: potentially update local Scheduler state to save from which Sequencer
        # `sequencer_id` the batch comes from 
        sequencer_id: _sequencer_id,
        epoch: _epoch,
        batch: _batch
      }} ->
        Debug.log("msg received: #{inspect(msg)} from node #{sender}")

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
    # mark as unfuzzable for now, since assume that the `send` messages from
    # Sequencers within a replica get delivered without delays to all Schedulers
    mark_unfuzzable()

    state = increment_processing_epoch(initial_state)

    # start accepting BatchTransactionMessage messages from the Sequencer components
    receive_batched_transaction_messages(state)
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
    Component.id(_replica=proc.replica, _partition=proc.partition, _type=proc.type)
  end

  @doc """
  Returns an unique ID for a Component given the replica, partition, and
  type of component
  """
  @spec id(atom(), non_neg_integer(), atom()) :: atom()
  def id(replica, partition, type) do
    List.to_atom(to_charlist(replica) ++ to_charlist(partition) ++ '-' ++ to_charlist(type))
  end

  @doc """
  When using ReplicationScheme.Async, returns whether a given component / process `proc` 
  is located on the main replica
  """
  @spec on_main_replica?(%Storage{} | %Sequencer{} | %Scheduler{}) :: boolean()
  def on_main_replica?(proc) do
    main_replica = proc.configuration.replication_scheme.main_replica
    replica = proc.replica
    if replica == main_replica do
      true
    else
      false
    end
  end

  @doc """
  When using ReplicationScheme.Raft, returns whether a given component / process `proc`
  is located on the leader replica
  """
  @spec on_leader_replica?(%Storage{} | %Sequencer{} | %Scheduler{}) :: boolean()
  def on_leader_replica?(proc) do
    # get which replica is the current leader for the given process
    # replication group
    leader_replica = ReplicationScheme.Raft.get_leader_for_partition(
      _replication_scheme=proc.configuration.replication_scheme,
      _partition=proc.partition
    )
    if proc.replica == leader_replica do
      true
    else
      false
    end
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
