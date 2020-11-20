# Simple CRUD transaction module. This treats and assumes
# transactions are single operations performed against a 
# Storage component that is a key-value store

defmodule Transaction do
  @enforce_keys [:type]

  defstruct(
    type: nil,
    key: nil,
    val: nil,
    timestamp: nil
  )

  @doc """
  Adds a timestamp to the transaction based on local system clock
  """
  @spec add_timestamp(%Transaction{}) :: %Transaction{}
  def add_timestamp(tx) do
    %{tx | timestamp: System.system_time}
  end

  @doc """
  CREATE transaction
  """
  @spec create(any(), any()) :: %Transaction{}
  def create(key, val) do
    %Transaction{
      type: :CREATE,
      key: key,
      val: val
    }
  end

  @doc """
  READ transaction
  """
  @spec read(any()) :: %Transaction{}
  def read(key) do
    %Transaction{
      type: :READ,
      key: key
    }
  end

  @doc """
  UPDATE transaction
  """
  @spec update(any(), any()) :: %Transaction{}
  def update(key, val) do
    %Transaction{
      type: :UPDATE,
      key: key,
      val: val
    }
  end

  @doc """
  DELETE transaction
  """
  @spec delete(any()) :: %Transaction{}
  def delete(key) do
    %Transaction{
      type: :DELETE,
      key: key
    }
  end
end