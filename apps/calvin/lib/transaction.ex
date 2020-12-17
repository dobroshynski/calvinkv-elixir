# Module for a Transaction Expression that can be part of a `write-type` 
# transaction and which gets evaluated at Transaction execution by a Scheduler
# when local and remote reads are collected

defmodule Transaction.Expression do
  @enforce_keys [:operand1, :operator, :operand2]

  defstruct(
    operand1: nil,
    operator: nil,
    operand2: nil,
    # the set of values that this Expression requires to be evaluated
    read_set: nil
  )

  @doc """
  Given a list of operands, generates a read set of values
  that are required to evaluate the Expression with the given
  operands
  """
  @spec generate_read_set([any()]) :: %MapSet{}
  def generate_read_set(operands) do
    Enum.reduce(operands, MapSet.new(),
      fn val, acc ->
        if is_atom(val) do
          MapSet.put(acc, val)
        else
          acc
        end
      end
    )
  end

  @doc """
  Given an expression, evaluates it and returns the result. So far supports
  basic arithmetic operations and basic boolean expressions
  """
  @spec eval(%Transaction.Expression{}, %{}) :: any()
  def eval(expr, all_collected_reads) do
    # bind the value to each operand variables, if necessary
    operand1 =
      if is_atom(expr.operand1), 
        do: Map.get(all_collected_reads, expr.operand1), 
        else: expr.operand1

    operand2 =
      if is_atom(expr.operand2), 
        do: Map.get(all_collected_reads, expr.operand2), 
        else: expr.operand2 

    case expr.operator do
      # arithmetic expressions
      :+ ->
        operand1 + operand2
      :- ->
        operand1 - operand2
      :* ->
        operand1 * operand2
      :/ ->
        operand1 / operand2

      # boolean expression
      :> ->
        operand1 > operand2
      :>= ->
        operand1 >= operand2       
      :< ->
        operand1 < operand2
      :<= ->
        operand1 <= operand2
      :== ->
        operand1 == operand2
      :!= ->
        operand1 != operand2
    end
  end

  @doc """
  Creates a new Transaction.Expression
  """
  @spec new(any(), atom(), any()) :: %Transaction.Expression{}
  def new(operand1, operator, operand2) do
    %Transaction.Expression{
      operand1: operand1,
      operator: operator,
      operand2: operand2,
      read_set: generate_read_set([operand1, operand2])
    }
  end
end

# Simple CRUD Transaction Operation module. This represents a simple 
# operation that can performed against a Storage component that is a
# key-value store

defmodule Transaction.Op do
  @enforce_keys [:type]

  defstruct(
    type: nil,
    key: nil,
    expr: nil
  )

  @doc """
  Evaluates, if needed, the Expression associated with the given `write`
  operation using a combined map of collected local and remote reads and 
  returns the updated operation
  """
  @spec evaluate_expr(%Transaction.Op{}, %{}) :: %Transaction.Op{}
  def evaluate_expr(op, all_collected_reads) do
    # evaluate the Expression and update the operation
    # and if the expression is a primitive value, simply 
    # return the operation unchanged
    case op.expr do
      %Transaction.Expression{} -> 
        val = Transaction.Expression.eval(op.expr, all_collected_reads)
        %{op | expr: val}
      _ ->
        op
    end
  end

  @doc """
  Returns a condensed representation of an Operation, which is a tuple of
  the Operation type and either just the key, or the key and the value 
  associated with the Operation
  """
  @spec condensed(%Transaction.Op{}) :: %Transaction.Op{}
  def condensed(op) do
    case op.type do
      type = :READ ->
        {type, op.key}
      
      type = :CREATE ->
        {type, op.key, op.expr}
      
      type = :UPDATE ->
        {type, op.key, op.expr}

      type = :DELETE ->
        {type, op.key}
    end
  end

  @doc """
  Given a `Transaction.Op` returns whether it is a `write` operation
  """
  @spec is_write?(%Transaction.Op{}) :: boolean()
  def is_write?(op) do
    case op.type do
      :READ -> 
        false
      :INVARIANT ->
        false
      _ ->
        true
    end
  end

  @doc """
  Given a `Transaction.Op` returns whether it is an `invariant` operation
  """
  @spec is_invariant?(%Transaction.Op{}) :: boolean()
  def is_invariant?(op) do
    case op.type do
      :INVARIANT -> 
        true
      _ ->
        false
    end
  end

  @doc """
  INVARIANT Operation which can act as a barrier to enforce needed safety conditions
  and can be used to execute a transaction abort if needed

  TODO: potentilly allow a custom 'operation' that will be executed if the invariant
  is true/false
  """
  @spec invariant(%Transaction.Expression{}) :: %Transaction.Op{}
  def invariant(expression) do
    %Transaction.Op{
      type: :INVARIANT,
      expr: expression
    }
  end

  @doc """
  CREATE Operation
  """
  @spec create(any(), %Transaction.Expression{} | any()) :: %Transaction.Op{}
  def create(key, expr) do
    %Transaction.Op{
      type: :CREATE,
      key: key,
      expr: expr
    }
  end

  @doc """
  READ Operation
  """
  @spec read(any()) :: %Transaction.Op{}
  def read(key) do
    %Transaction.Op{
      type: :READ,
      key: key
    }
  end

  @doc """
  UPDATE Operation
  """
  @spec update(any(), %Transaction.Expression{} | any()) :: %Transaction.Op{}
  def update(key, expr) do
    %Transaction.Op{
      type: :UPDATE,
      key: key,
      expr: expr
    }
  end

  @doc """
  DELETE Operation
  """
  @spec delete(any()) :: %Transaction.Op{}
  def delete(key) do
    %Transaction.Op{
      type: :DELETE,
      key: key
    }
  end
end

# Module representing a Transaction in the system. A single Transaction
# consists of a series of `Transaction.Op` operations that are executed
# against a Storage component

defmodule Transaction do
  alias __MODULE__

  @enforce_keys [:operations, :read_set, :write_set]

  defstruct(
    operations: nil,
    timestamp: nil,

    # read and write sets of the Transaction
    read_set: nil,
    write_set: nil,

    # participating partitions for this Transaction
    # as well as which are active and passive participants
    participating_partitions: nil,
    active_participants: nil,
    passive_participants: nil,

    # for timing of Transaction start / execution from
    # client to Storage
    started: nil,
    finished: nil,
    duration: nil,

    # optional Transaction id for debugging purposes
    id: nil
  )

  @doc """
  Sets a timestamp to when this Transaction was started, being sent
  by a client to a Sequencer component
  """
  @spec set_started(%Transaction{}, non_neg_integer()) :: %Transaction{}
  def set_started(tx, time) do
    %{tx | started: time}
  end

  @doc """
  Sets a timestamp to when this Transaction executed against a Storage
  component and sets the duration based on `tx.started` and `tx.finished`
  """
  @spec set_finished(%Transaction{}, non_neg_integer(), atom()) :: %Transaction{}
  def set_finished(tx, time, physical_node) do
    # duration in microseconds
    duration_mus = DateTime.diff(time, tx.started, :microsecond)
    # duration in milliseconds
    duration_ms = duration_mus / 1000

    Debug.Timing.log("{#{tx.id}} executed in #{duration_ms}ms on #{physical_node}")

    %{tx |
      finished: time, 
      duration: duration_ms
    }
  end

  @doc """
  Adds a timestamp to the Transaction based on local system clock
  """
  @spec add_timestamp(%Transaction{}) :: %Transaction{}
  def add_timestamp(tx) do
    %{tx | timestamp: DateTime.utc_now()}
  end

  @doc """
  Sets the participating partitions for this Transaction to the given set of partitions
  """
  @spec set_participating_partitions(%Transaction{}, %MapSet{}, %MapSet{}, %MapSet{}) :: %Transaction{}
  def set_participating_partitions(tx, active, passive, all) do
    %{tx | 
      active_participants: active,
      passive_participants: passive,
      participating_partitions: all
    }
  end


  @doc """
  Given a list of `Transaction.Op` Transaction operations, generates
  a read set for the operations, which is a set of values that the
  operations perform any type of read on
  """
  @spec generate_read_set([%Transaction.Op{}]) :: %MapSet{}
  def generate_read_set(operations) do
    # reduce all of the operations into a set by 
    # adding to set if the operation is a READ
    Enum.reduce(operations, MapSet.new(),
      fn op, acc ->
        case op.type do
          :READ ->
            MapSet.put(acc, op.key)
          type when type in [:CREATE, :UPDATE, :INVARIANT] ->
            case op.expr do
              %Transaction.Expression{} ->
                MapSet.union(acc, op.expr.read_set)
              _ ->
                acc
            end
          _ ->
            acc
        end
      end
    )
  end

  @doc """
  Given a list of `Transaction.Op` Transaction operations, generates
  a write set for the operations, which is a set of values that the
  operations perform any type of write on
  """
  @spec generate_write_set([%Transaction.Op{}]) :: %MapSet{}
  def generate_write_set(operations) do
    # reduce all of the operations into a set by adding to set
    # if the operation is a CREATE, UPDATE, or DELETE
    Enum.reduce(operations, MapSet.new(), 
      fn op, acc ->
        case op.type do
          :CREATE ->
            MapSet.put(acc, op.key)
          :UPDATE ->
            MapSet.put(acc, op.key)
          :DELETE ->
            MapSet.put(acc, op.key)
          _ -> 
            acc
        end
      end
    )
  end

  @doc """
  Creates a new Transaction with given operations and a given id for
  debugging purposes
  """
  @spec new([%Transaction.Op{}], atom()) :: %Transaction{}
  def new(operations, id) do
    tx = Transaction.new(operations)
    %{tx | id: id}
  end

  @doc """
  Creates a new Transaction with given operations and generates the read
  and write sets for the Transaction based on the Operations that make up
  the Transaction
  """
  @spec new([%Transaction.Op{}]) :: %Transaction{}
  def new(operations) do
    %Transaction{
      operations: operations,
      read_set: Transaction.generate_read_set(operations),
      write_set: Transaction.generate_write_set(operations)
    }
  end
end
