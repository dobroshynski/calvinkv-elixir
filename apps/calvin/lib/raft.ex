# Module for managing Raft state as part of a Sequencer RSM that is using
# Raft-based synchronous replication of Transaction batches during
# each epoch. Every Sequencer RSM maintains an instance of the Raft
# struct to hold state information about the current Raft configuration
# for that Sequencer

defmodule Raft do
  @enforce_keys [:log, :current_term, :commit_index, :last_applied]

  import Emulation, only: [send: 2, timer: 1, whoami: 0, cancel_timer: 1, timer: 2]
  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  # Module for a Raft log comprising of Log.Entry entries. The log is 1-index based, and the
  # index in the Log corresponds to the epoch of the batch stored in the Entry at that index

  # For example, given Transactions [tx1, tx2] batched during epoch 1 and Transactions
  # [tx3, tx4] batched during epoch 2, the Log contains 2 entries in `entries`
  # with epoch 1 batch at index 1 and epoch 2 batch at index 2

  defmodule Log do
    @enforce_keys [:entries]

    defstruct(
      entries: nil
    )

    @doc """
    Creates a new empty Raft log
    """
    @spec new() :: %Log{}
    def new() do
      %Log{
        entries: []
      }
    end

    @doc """
    Given a Log and a 1-based index (likely corresponding to an epoch), returns the Log.Entry 
    entry at that index
    """
    @spec get_entry_at_index(%Log{}, non_neg_integer()) :: :no_entry | %Log.Entry{}
    def get_entry_at_index(log, idx) do
      if idx <= 0 || length(log.entries) < idx do
        :noentry
      else
        # since the indexing is meant to be 1-based in order to
        # match up to epoch numbers, offset the given index by 1
        Enum.at(log.entries, idx - 1)
      end
    end

    @doc """
    Given a Log and a 1-based index (likely corresponding to an epoch), returns a suffix of
    Log.Entry entries starting at the given index
    """
    @spec get_entries_at_index(%Log{}, non_neg_integer()) :: [%Log.Entry{}]
    def get_entries_at_index(log, idx) do
      if idx > length(log.entries) do
        []
      else
        Enum.slice(log.entries, idx - 1, length(log.entries))
      end
    end

    @doc """
    Given a Log, returns the index for the last log entry
    """
    @spec get_last_log_index(%Log{}) :: non_neg_integer()
    def get_last_log_index(log) do
      Enum.at(log.entries, length(log.entries) - 1, Log.Entry.empty()).index
    end

    @doc """
    Given a Log, returns the Raft term for the last log entry
    """
    @spec get_last_log_term(%Log{}) :: non_neg_integer()
    def get_last_log_term(log) do
      Enum.at(log.entries, length(log.entries) - 1, Log.Entry.empty()).term
    end

    @doc """
    Given a Log and a 1-based index (likely corresponding to an epoch), removes all 
    log entries at the given index and larger, and returns the updated Log
    """
    @spec remove_entries(%Log{}, non_neg_integer()) :: %Log{}
    def remove_entries(log, idx) do
      if idx > length(log.entries) do
        log
      else
        # compute how many entries to remove and multiply by -1
        # since need to remove from the back of the log
        num_drop = length(log.entries) - idx + 1
        %{log | entries: Enum.drop(log.entries, -num_drop)}
      end
    end

    @doc """
    Given a Log and a list of `Log.Entry` entries, appends the entries to the
    Log and returns the updated Log
    """
    @spec add_entries(%Log{}, [%Log.Entry{}]) :: %Log{}
    def add_entries(log, entries) do
      %{log | entries: log.entries ++ entries}
    end

    @doc """
    Given a Log and a 1-based index, returns whether the log entry at the index
    exists
    """
    @spec logged?(%Log{}, non_neg_integer()) :: boolean()
    def logged?(log, idx) do
      if idx > 0 && idx <= length(log.entries) do
        true
      else
        false
      end
    end

    @doc """
    Given a Log, incoming entries, and additional data about incoming entries, update the log
    to reflect the correct entries
    """
    @spec update_with_incoming_entries(%Log{}, [%Log.Entry{}], %Log.Entry{}, %Log.Entry{}, non_neg_integer()) :: %Log{}
    def update_with_incoming_entries(log, entries, earliest_new_entry, latest_current_entry, prev_log_index) do
      # if an empty local log so far, simply append new entries
      if latest_current_entry == :noentry do
        Log.add_entries(log, entries)
      else
        # &5.3, step 3 of receiver implementation of AppendEntries RPC
        # if prevLogIndex is 0, that means that the leader is sending every
        # log entry that it has, so just clear everything from log index = 1
        # and try to append, if prevLogIndex is not 0 but the terms don't match,
        # then clear everything from the index after prevLogIndex and append too
        # which is the same logic
        if prev_log_index == 0 &&
            earliest_new_entry.term == latest_current_entry.term do
          # delete the existing entry and all that follow it 
          log = Log.remove_entries(log, prev_log_index + 1)
          Log.add_entries(log, entries)
        else
          if earliest_new_entry.term != latest_current_entry.term do
            # delete the existing entry and all that follow it and 
            # append new entries
            log = Log.remove_entries(log, prev_log_index + 1)
            Log.add_entries(log, entries)
          else
            Log.add_entries(log, entries)
          end
        end
      end
    end

    @doc """
    Given a Log, an index for a Raft.Entry, and an expected term of the Raft.Entry,
    returns whether the actual log entry's term at the `index` matches the given
    expected `term`
    """
    @spec entry_matches?(%Log{}, non_neg_integer(), non_neg_integer()) :: boolean()
    def entry_matches?(log, index, expected_term) do
      # this will be true if this is the very first append request attempt
      # and both logs are empty
      if index == 0 do
        # this means that the leader is sending it's entire log, since prevLogIndex of
        # 0 coming from the leader means that the first entry that it is sending is starting
        # at index 1 which is the beginning of the log
        true
      else
        # first check if the index exists in current log, then if the terms match at that
        # index
        if Log.logged?(log, index) && Log.get_entry_at_index(log, index).term == expected_term do
          true
        else
          false
        end
      end
    end

    # Module for a single Raft log entry in the system. Each individual entry is a batch
    # of Transactions for a certain epoch

    defmodule Entry do
      @enforce_keys [:index, :term, :batch]

      defstruct(
        index: nil,
        term: nil,
        batch: nil
      )

      @doc """
      Creates a new empty log Entry
      """
      @spec empty() :: %Log.Entry{}
      def empty() do
        %Log.Entry{index: 0, term: 0, batch: []}
      end

      @doc """
      Creates a new log Entry with a given batch of Transactions, for a given
      index in the Log, and during a given Raft term
      """
      @spec new(non_neg_integer(), non_neg_integer(), [%Transaction{}]) :: %Log.Entry{}
      def new(index, term, tx_batch) do
        %Log.Entry{
          index: index,
          term: term,
          batch: tx_batch
        }
      end
    end
  end

    # AppendEntries RPC message from the Raft protocol

    defmodule AppendEntries do
      @enforce_keys [
        :term,
        :leader_id,
        :prev_log_index,
        :prev_log_term,
        :leader_commit_index,
        :entries
      ]
  
      defstruct(
        term: nil,
        leader_id: nil,
        prev_log_index: nil,
        prev_log_term: nil,
        leader_commit_index: nil,
        entries: nil,
      )
  
      @doc """
      Creates a new AppendEntries
      """
      @spec new(non_neg_integer(), atom(), non_neg_integer(), non_neg_integer(), non_neg_integer(), [%Log.Entry{}]) :: %AppendEntries{}
      def new(term, leader_id, prev_log_index, prev_log_term, leader_commit_index, entries) do
        %AppendEntries{
          term: term,
          leader_id: leader_id,
          prev_log_index: prev_log_index,
          prev_log_term: prev_log_term,
          leader_commit_index: leader_commit_index,
          entries: entries
        }
      end
    end
  
    # AppendEntries RPC response message from the Raft protocol
  
    defmodule AppendEntries.Response do
      @enforce_keys [:replica, :term, :log_index, :success]
  
      defstruct(
        replica: nil,
        term: nil,
        log_index: nil,
        success: nil
      )
  
      @doc """
      Creates a new AppendEntries.Response
      """
      @spec new(non_neg_integer(), non_neg_integer(), non_neg_integer(), boolean()) :: %AppendEntries.Response{}
      def new(replica, term, log_index, success) do
        %AppendEntries.Response{
          replica: replica,
          term: term,
          log_index: log_index,
          success: success
        }
      end
    end

  defstruct(
    # either `leader` or `follower`
    current_role: nil,
    current_leader: nil,

    # view of the Raft replication group - list of partitions
    # from 1..num_replicas where `num_replicas` is given by
    # the current Configuration
    view: nil,
  
    # id / number of this Raft state replication group - identified
    # by the partition of the Sequencer process running the Raft
    # protocol for replication
    my_replica: nil,
    replica_group_id: nil,

    # persistent state on all RSMs
    current_term: nil,

    # volatile state on all RSMs
    commit_index: nil,
    last_applied: nil,

    # map of nextIndex[] {replica -> nextIndex}
    next_index: nil,
    # map of matchIndex[] {replica -> matchIndex}
    match_index: nil,

    # local Raft log
    log: nil
  )

  @doc """
  Returns the current role of a given Raft state
  """
  @spec current_role?(%Raft{}) :: atom()
  def current_role?(state) do
    state.current_role
  end

  # Module for generic utility functions when managing a Raft state

  defmodule Utils do

    @doc """
    Returns every replica in the given Raft state replication
    group other than the caller Raft state
    """
    @spec everyone_other_than_me(%Raft{}) :: [atom()]
    def everyone_other_than_me(state) do
      state.view |> Enum.filter(fn replica -> replica != state.my_replica end)
    end

    @doc """
    Helper function for a Raft state in `leader` role to determine whether a majority of
    matchIndex[i] >= some new proposed commit index N, where matchIndex[i] are the most
    up-to-date indexes for each member of replication group for entries known to be replicated
    on that RSM
    """
    @spec majority_match?(%Raft{}, non_neg_integer()) :: boolean()
    def majority_match?(state, n) do
      # compute the count of matchIndex entries that are >= N
      count =
        Enum.reduce(state.match_index, 0, fn {_pid, match}, acc ->
          if match >= n do
            acc + 1
          else
            acc
          end
        end)
  
      # how many 'counts' are needed for majority
      majority = round(length(state.view) / 2)
  
      if count >= majority do
        true
      else
        false
      end
    end
  end

  # Module for managing a Raft state that is currently in `leader`
  # role

  defmodule Leader do

    @doc """
    Given a Raft state, returns the state in `leader` role
    with necessary state updated
    """
    @spec make(%Raft{}) :: %Raft{}
    def make(state) do
      IO.puts("making leader")

      # nextIndex[] initialization like in the Raft paper
      # (leader last log index + 1)
      log_index = Log.get_last_log_index(state.log) + 1

      # re-initialize nextIndex[]
      next_index = state.view
        |> Enum.map(fn partition -> {partition, log_index} end)
        |> Map.new()

      # re-initialize matchIndex[]
      match_index = state.view
        |> Enum.map(fn partition -> {partition, 0} end)
        |> Map.new()
      
      state = %{state | 
        current_role: :leader,
        current_leader: state.my_replica,
        next_index: next_index, 
        match_index: match_index
      }

      # send a heartbeat AppendEntries RPC to let everyone know
      # that this Raft state is the leader
      Raft.Leader.broadcast_heartbeat_rpc(state)

      # return the updated state
      state
    end

    @doc """
    Given a Raft state in `leader` role, returns whether can
    commit anything
    """
    @spec can_commit_next?(%Raft{}) :: boolean()
    def can_commit_next?(state) do
      if state.commit_index > state.last_applied do
        true
      else
        false
      end
    end

    @doc """
    Given a Raft state in `leader` role, prepares the Raft
    state to commit the next available Raft log entry
    """
    @spec prepate_to_commit(%Raft{}) :: %Raft{}
    def prepate_to_commit(state) do
      IO.puts("[leader #{whoami()}] commit index of #{state.commit_index} > last applied: #{state.last_applied}, can commit!")

      # update the state with new lastApplied
      state = %{state | last_applied: state.last_applied + 1}
    end

    @doc """
    Given a Raft state in `leader` role, and information about a replication
    attempt to a replica `for_replica`, updates the records stored on the Raft
    state for nextIndex[] and matchIndex[]
    """
    @spec update_records(%Raft{}, atom(), boolean(), non_neg_integer()) :: %Raft{}
    def update_records(state, for_replica, with_success?, highest_index_matched) do
      if with_success? == true do
        next_to_send = highest_index_matched + 1
  
        updated_next_index = Map.put(state.next_index, for_replica, next_to_send)
        updated_match_index = Map.put(state.match_index, for_replica, highest_index_matched)
  
        %{state | 
          next_index: updated_next_index,
          match_index: updated_match_index
        }
      else
        # decrement nextIndex by 1 and retry later
        next_to_send = Map.get(state.next_index, for_replica) - 1
        updated_next_index = Map.put(state.next_index, for_replica, next_to_send)

        %{state | next_index: updated_next_index}
      end
    end

    @doc """
    Given a Raft state in `leader` role, attempt to update the commit index
    of the state to current commit index + 1
    """
    @spec update_commit_index(%Raft{}) :: %Raft{}
    def update_commit_index(state) do
      n = state.commit_index + 1

      IO.puts("checking if can update commit index to #{n}...")
  
      # trying to update the commit index on leader to N
      # conditions from &5.3, &5.4 needed to update the commit index
      if Raft.Utils.majority_match?(state, n) && Log.logged?(state.log, n) &&
           Log.get_entry_at_index(state.log, n).term == state.current_term do
  
        IO.puts("current matchIndex: #{inspect(state.match_index)}")
        IO.puts("successfully updated commit index to #{n}")
        # set commitIndex = N
        %{state | commit_index: n}
      else
        state
      end
    end

    @doc """
    Given a Raft state in `leader` role, broadcasts a heartbeat AppendEntries
    RPC message to all followers in the current replication group
    """
    @spec broadcast_heartbeat_rpc(%Raft{}) :: no_return()
    def broadcast_heartbeat_rpc(state) do
      # create a heartbeat request with entries = nil
      heartbeat_request =
        Raft.AppendEntries.new(
          state.current_term,
          state.my_replica,
          0,
          0,
          state.commit_index,
          nil
        )
  
      IO.puts("preparing to broadcast AppendEntries: #{inspect(heartbeat_request)}")
  
      # broadcast an empty heartbeat AppendEntries RPC to all followers
      follower_replicas = Raft.Utils.everyone_other_than_me(state)

      Enum.map(follower_replicas, 
        fn replica ->
          # construct a unique id for a recipient Sequencer component within 
          # the replication group
          sequencer_id = Component.id(_replica=replica, _partition=state.replica_group_id, _type=:sequencer)
          
          # send the heartbeat request to the member of the replication group
          send(sequencer_id, heartbeat_request)
  
          IO.puts("[node #{whoami()}] sent AppendEntries broadcast message to #{sequencer_id}:
          #{inspect(heartbeat_request)}")
        end
      )
    end

    # Message handler functions for a Raft state in `leader` mode

    defmodule Handler do

      @doc """
      Handler function for an AppendEntries request
      """
      @spec append_entries_request(%Raft{}, %Raft.AppendEntries{}, atom()) :: %Raft{}
      def append_entries_request(state, request, sender) do
        # return the state unchanged
        state
      end

      @doc """
      Handler function for an AppendEntries response which processes the message and determines
      how to update the local `leader` state as well as potentially re-send AppendEntries request
      to the processes that are currently in `follower` role for the current replication group
      """
      @spec append_entries_response(%Raft{}, %Raft.AppendEntries.Response{}, atom()) :: %Raft{}
      def append_entries_response(state, response, sender) do
        IO.puts("[leader #{whoami()}] handling from PID: #{sender} Response: #{inspect(response)}")

        IO.puts("[leader #{whoami()}] current state of nextIndex[]: #{inspect(state.next_index)}")
        IO.puts("[leader #{whoami()}] current state of matchIndex[]: #{inspect(state.match_index)}")

        state = Raft.Leader.update_records(state, 
          _for_replica=response.replica, 
          _with_success?=response.success, 
          _highest_index_matched=response.log_index
        )

        IO.puts("[leader #{whoami()}] updated state of nextIndex[]: #{inspect(state.next_index)}")
        IO.puts("[leader #{whoami()}] updated state of matchIndex[]: #{inspect(state.match_index)}")

        if response.success do
          IO.puts("[leader #{whoami()}] successfull AppendEntries.Response from #{sender}")
    
          # given the updated state with matchIndex[] potentially updated, check
          # if we can update the commit index locally for the leader
          state = Raft.Leader.update_commit_index(state)
          
          # return the updated state
          state
        else
          IO.puts("[leader #{whoami()}] unsuccessfull AppendEntries.Response")
    
          # respond to the follower (`sender`) by sending another AppendEntries request
          # message which now will contain a nextIndex which is decremented by 1
          # by the `Raft.Leader.update_records/3` function
          updated_next_index_for_follower = Map.get(state.next_index, response.replica)
    
          # updated index of log entry preceding new ones and updated term 
          # of the entry at index preceding new entries
          {updated_prev_index, updated_prev_term} = Raft.get_preceding_index_and_term(
            state,
            updated_next_index_for_follower
          )
    
          # get the log entries starting at the updated nextIndex for the follower Raft
          # state
          log_entries_starting_at_next_index =
            Log.get_entries_at_index(state.log, updated_next_index_for_follower)
    
          # create and send the updated AppendEntries request to the follower
          updated_append_request =
            Raft.AppendEntries.new(
              state.current_term,
              state.my_replica,
              updated_prev_index,
              updated_prev_term,
              log_entries_starting_at_next_index,
              state.commit_index
            )
          # send the updated AppendEntries
          send(sender, updated_append_request)
          
          # return the state unchanged
          state
        end
      end
    end
  end

  # Module for managing a Raft state that is currently in `follower`
  # role

  defmodule Follower do

    @doc """
    Given a Raft state, returns the state in `follower` role
    with necessary state updated
    """
    @spec make(%Raft{}) :: %Raft{}
    def make(state) do
      IO.puts("making follower")

      %{state | current_role: :follower}
    end

    @doc """
    Given a Raft state in `follower` role, returns whether can
    apply anything to local state
    """
    @spec can_apply_next?(%Raft{}) :: boolean()
    def can_apply_next?(state) do
      if state.commit_index > state.last_applied do
        true
      else
        false
      end
    end

    @doc """
    Given a Raft state in `follower` role, prepares the Raft
    state to apply the next available Raft log entry locally
    """
    @spec prepate_to_apply(%Raft{}) :: %Raft{}
    def prepate_to_apply(state) do
      IO.puts("[follower #{whoami()}] commit index is higher than last applied, so apply next operation")

      # update the state with new lastApplied
      state = %{state | last_applied: state.last_applied + 1}
    end

    @doc """
    Given a Raft state in `follower` role and a commit index of the Raft
    state that is the leader of the current replication group, attempt to
    update the local commit index
    """
    @spec update_commit_index(%Raft{}, non_neg_integer()) :: %Raft{}
    def update_commit_index(state, leader_commit_index) do
      if leader_commit_index > state.commit_index do
        new_commit_index = min(leader_commit_index, Log.get_last_log_index(state.log))
        IO.puts("[follower #{whoami()}] updating commit index from #{state.commit_index} -> #{new_commit_index}")
        
        %{state | commit_index: new_commit_index}
      else
        # no update to commit index necessary
        state
      end
    end

    # Message handler functions for a Raft state in `follower` mode

    defmodule Handler do

      @doc """
      Handler function for an AppendEntries request which processes the RPC request and
      determines how to update the local state as well as respond to the sender that is
      in `leader` role for the current replication group 
      """
      @spec append_entries_request(%Raft{}, %Raft.AppendEntries{}, atom()) :: %Raft{}
      def append_entries_request(state, request, sender) do
        cond do
          # handle the heartbeat AppendEntries RPC, since entries == nil signifies
          # that the Raft state that is currently in `leader` role is sending a heartbeat 
          # and not trying to append anything
          request.entries == nil ->
            IO.puts("[follower #{whoami()}] received heartbeat RPC from #{sender}, going to attempt to apply anything locally")
            
            # check if can update the local commit index in case the current leader has
            # moved up it's commit index since the last AppendEntries request RPC
            state = Raft.Follower.update_commit_index(state, _commit_index=request.leader_commit_index)

            # return the updated state
            state

          # handle case in &5.3 of receiver implementation - if the local log does not contain
          # a term at prevLogIndex which matches prevLogTerm, reply false. The sender Raft state
          # that is in `leader` role should then decrement nextIndex for this member of the
          # replication group and try re-sending an AppendEntries RPC
          Log.entry_matches?(state.log, request.prev_log_index, request.prev_log_term) == false ->
            # respond with success = false since the entry in Raft log did not match
            response = Raft.AppendEntries.Response.new(
              state.current_term,
              request.prev_log_index,
              false
            )

            # send the AppendEntries response message back to the Sequencer that is currently
            # the leader of the current replication group
            send(sender, response)

            # return the state unchanged
            state

          # the previous entry in log matches on index and term thus handle an attempt by the
          # Raft state that is currently in `leader` role to replicate new entries to the log
          Log.entry_matches?(state.log, request.prev_log_index, request.prev_log_term) ->

            # entries that the `leader` of the current replication group is attempting
            # to replicate to the current follower
            entries = request.entries

            IO.puts("[follower #{whoami()}] received a valid AppendEntrues RPC from #{sender}
            attempting to append entries: #{inspect(entries)}")
    
            # latest of entries that have been sent by the leader, this entry has the
            # highest index of the entries that the `leader` is attempting to replicate to
            # this `follower`
            latest_new_entry = List.last(entries)
    
            # the 'oldest' entry in `entries` having the lowest index
            earliest_new_entry = List.first(entries)
    
            # get the latest entry in the follower's local Raft log so far
            latest_current_entry = Log.get_entry_at_index(state.log, request.prev_log_index + 1)

            # update the local Raft log with incoming entries and update the Raft state 
            state = %{state | 
              log: Raft.Log.update_with_incoming_entries(
                state.log, 
                entries,
                earliest_new_entry,
                latest_current_entry,
                request.prev_log_index
              )
            }
    
            IO.puts("updated local log to: #{inspect(state.log)}")

            # send back an AppendEntries response signaling an OK that the entries sent by the
            # Raft process `sender` in `leader` role have successfully been replicated to the Raft
            # log on this process. With the response, the leader `sender` will know that at this
            # point, this follower has extended it's local Raft log up to the log entry with the
            # latest index sent in `entries`

            response_msg = Raft.AppendEntries.Response.new(
              state.my_replica,
              state.current_term,
              # sending the **index of the latest entry known to be replicated here**
              latest_new_entry.index,
              true
            )

            # send the message back to the Sequencer that is the leader of the current
            # replication group
            send(sender, response_msg)

            # attempt to update the commit index locally given the `leader_commit_index`
            # received in this AppendEntries RPC
            state = Raft.Follower.update_commit_index(state, _commit_index=request.leader_commit_index)

            # return the updated state
            state
        end
      end
    end
  end

  @doc """
  Returns the index and the term of the entry immediately preceding the entry at 
  `index`. Used when sending the prevLogIndex and prevLogTerm with an AppendEntries RPC
  """
  @spec get_preceding_index_and_term(%Raft{}, non_neg_integer()) :: {integer(), integer()}
  def get_preceding_index_and_term(state, index) do
    preceding_index = index - 1
    preceding_entry = Log.get_entry_at_index(state.log, preceding_index)

    # if the entry preceding the one at index is :noentry
    # that means the process is sending from the beginning of the log

    if preceding_entry == :noentry do
      {preceding_index, state.current_term}
    else
      {preceding_index, preceding_entry.term}
    end
  end

  @doc """
  Given a Raft state for a Sequencer RSM residing on the current leader replica and a batch of
  Transactions, prepares the state for replication via AppendEntries RPC by updating the necessary
  state. Extends the local log on the Raft state in `leader` role with the newly created Raft log 
  entry and updates the matchIndex for the `leader` state
  """
  @spec prepare_to_replicate(%Raft{}, [%Transaction{}]) :: %Raft{}
  def prepare_to_replicate(state, tx_batch) do
    # create a Raft log entry with the given batch, which the `leader` Raft state will
    # attempt to replicate to enough `follower` states before marking it as committed 
    entry = %Log.Entry{
      index: Log.get_last_log_index(state.log) + 1,
      term: state.current_term,
      batch: tx_batch
    }
    IO.puts("[node #{whoami()}] created a new Log.Entry: #{inspect(entry)}")

    # extend the local `leader` log by appending the new entries
    state = %{state | log: Log.add_entries(state.log, [entry])}
    IO.puts("[node #{whoami()}] updated local log to: #{inspect(state.log)}")
    
    # now also update the matchIndex for current server (leader), since
    # we extended it
    new_match_index = Log.get_last_log_index(state.log)
    state = %{state | match_index: Map.put(state.match_index, state.my_replica, new_match_index)}

    IO.puts("done preparing for replication, updated matchIndex to: #{inspect(state.match_index)}")
    
    state
  end

  @doc """
  Given a Raft state for a Sequencer RSM residing on the current leader replica, replicates the
  appropriate Raft log entries via an AppendEntries RPC to every Raft state in a `follower` role
  that is in the current replication group
  """
  @spec replicate(%Raft{}) :: %Raft{}
  def replicate(state) do
    IO.puts("replicating...")

    # the the index of the latest log entry on the local Raft log
    last_log_index = Log.get_last_log_index(state.log)

    # get all of the Raft states in the current replication group
    followers = Raft.Utils.everyone_other_than_me(state)

    followers |> Enum.map(
      fn replica ->
        # get nextIndex for the follower member of replica group
        next_index_for_follower = Map.get(state.next_index, replica)

        IO.puts("nextIndex for #{replica}: #{next_index_for_follower}")

        if last_log_index >= next_index_for_follower do
          # get the log entries that need to be sent to this follower server
          log_entries_starting_at_next_index = Log.get_entries_at_index(state.log, next_index_for_follower)

          IO.puts("entries that need to send to #{replica}: 
          #{inspect(log_entries_starting_at_next_index)}")

          # compute the index of log entry immediately preceding new ones
          # and term of that prevLogIndex entry
          {preceding_index, preceding_term} =
            Raft.get_preceding_index_and_term(state, next_index_for_follower)

          # construct an AppendEntries request
          append_request = Raft.AppendEntries.new(
            state.current_term,
            state.my_replica,
            preceding_index,
            preceding_term,
            state.commit_index,
            log_entries_starting_at_next_index
          )
          
          # construct a unique id for a recipient Sequencer component within 
          # the replication group
          follower_sequencer_id = Component.id(_replica=replica, _partition=state.replica_group_id, _type=:sequencer)
          
          # send to the message
          send(follower_sequencer_id, append_request)

          IO.puts("sent to #{follower_sequencer_id} AppendEntries RPC: #{inspect(append_request)}")
        end
      end
    )

    # return the state unchanged
    state
  end

  @doc """
  Creates a new Raft state instance based on a given Sequencer RSM
  """
  @spec init(%Sequencer{}) :: %Raft{}
  def init(sequencer_state) do
    # start with a basic Raft config
    basic_config = Raft.init()

    # set the replica view and replication group
    replica_view = ReplicationScheme.get_replica_view(sequencer_state.configuration.replication_scheme)
    basic_config = %{basic_config | view: replica_view}

    # set the replication group and own replica
    basic_config = %{basic_config | 
      replica_group_id: sequencer_state.partition,
      my_replica: sequencer_state.replica
    }

    # get the initial leader replica based on the Configuration
    leader_replica = ReplicationScheme.Raft.get_leader_for_partition(
      _replication_scheme=sequencer_state.configuration.replication_scheme, _partition=sequencer_state.partition
    )

    if leader_replica == sequencer_state.replica do
      # the given Sequencer process is on the replica that has been configured
      # to be the leader replica at launch

      %{basic_config | 
        current_role: :leader,
        current_leader: leader_replica
      }
    else
      # the given Sequencer process is **not** on the replica that has been configured
      # to be the leader replica at launch

      %{basic_config | 
        current_role: :follower,
        current_leader: leader_replica
      }
    end
  end

  @doc """
  Creates a new Raft state instance in basic configuration
  """
  @spec init() :: %Raft{}
  def init() do
    %Raft{
      log: Log.new(),
      current_term: 1,
      commit_index: 0,
      last_applied: 0
    }
  end
end