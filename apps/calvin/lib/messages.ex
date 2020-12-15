# Module for a message that Sequencers on the main replica send to other
# Sequencers in their replication group to asynchronously replicate the
# Transaction batch input for an epoch

# assumes that every ReplicateBatch request for every `epoch` gets delivered
# as expected, so the replicas do not get out of sync and epochs on replicas get incremented
# roughly at the same pace. For synchronous replication, can use the Raft-based synchronous
# replication with Raft.AppendEntries requests

defmodule Async.ReplicateBatch do
  @enforce_keys [:epoch, :batch]

  defstruct(
    epoch: nil,
    batch: nil
  )

  @doc """
  Creates a new Async.ReplicateBatch message
  """
  @spec new(non_neg_integer(), [%Transaction{}]) :: %Async.ReplicateBatch{}
  def new(epoch, batch) do
    %Async.ReplicateBatch{
      epoch: epoch,
      batch: batch
    }
  end
end

# Message of batched Transactions that gets sent by the Sequencer to the
# Scheduler components in the same replica for tx execution

defmodule BatchTransactionMessage do
  @enforce_keys [:sequencer_id, :epoch, :batch]

  defstruct(
    sequencer_id: nil,
    epoch: nil,
    batch: nil
  )

  @doc """
  Creates a new BatchTransactionMessage
  """
  @spec new(atom(), non_neg_integer(), [%Transaction{}]) :: %BatchTransactionMessage{}
  def new(sequencer_id, epoch, batch) do
    %BatchTransactionMessage{
      sequencer_id: sequencer_id,
      epoch: epoch,
      batch: batch
    }
  end
end

# Message that Scheduler components send to other actively participating Schedulers
# when executing a Transaction to serve the local reads, which is a map of {key -> value}

defmodule LocalReadsTransactionMessage do
  @enforce_keys [:local_reads]

  defstruct(
    local_reads: nil
  )

  @doc """
  Creates a new LocalReadsTransactionMessage
  """
  @spec new(%{}) :: %LocalReadsTransactionMessage{}
  def new(local_reads) do
    %LocalReadsTransactionMessage{
      local_reads: local_reads
    }
  end
end
