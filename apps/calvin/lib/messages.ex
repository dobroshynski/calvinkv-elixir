# Module for a message that Sequencers on the main replica send to other
# Sequencers in their replication group to asynchronously replicate the
# Transaction batch input for an epoch

# TODO: async replication mode supported only
# assumes that every ReplicateBatchRequest for every `epoch` gets delivered
# as expected, so the replicas do not get out of sync and epochs on replicas get incremented
# roughly at the same pace. This should be adjusted for async network model where messages can
# be dropped, delayed, and re-ordered

defmodule AsyncReplicateBatchRequest do
  @enforce_keys [:epoch, :batch]

  defstruct(
    epoch: nil,
    batch: nil
  )

  @doc """
  Creates a new AsyncReplicateBatchRequest
  """
  @spec new(non_neg_integer(), [%Transaction{}]) :: %AsyncReplicateBatchRequest{}
  def new(epoch, batch) do
    %AsyncReplicateBatchRequest{
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
