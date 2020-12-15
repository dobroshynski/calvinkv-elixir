defmodule TransactionTest do
  use ExUnit.Case
  doctest Transaction
  doctest Transaction.Op

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Transaction read set is generated as expected" do
    # test single-op Transactions
    tx1 = Transaction.new(_operations=[Transaction.Op.create(:a, 1)])
    tx2 = Transaction.new(_operations=[Transaction.Op.update(:a, 0)])
    tx3 = Transaction.new(_operations=[Transaction.Op.delete(:a)])
    tx4 = Transaction.new(_operations=[Transaction.Op.read(:a)])

    # check that the read sets are as expected
    assert MapSet.size(tx1.read_set) == 0
    assert MapSet.size(tx2.read_set) == 0
    assert MapSet.size(tx3.read_set) == 0

    assert MapSet.size(tx4.read_set) == 1
    assert MapSet.member?(tx4.read_set, :a) == true

    # test multi-op Transactions
    # create a Transaction with a couple of operations
    tx = Transaction.new(_operations=[
        Transaction.Op.create(:a, 1),
        Transaction.Op.create(:b, 1),
        Transaction.Op.update(:a, 0),
        Transaction.Op.read(:a),
        Transaction.Op.delete(:a),
    ])
    # check that the read set is as expected
    read_set = tx.read_set

    assert MapSet.size(read_set) == 1
    assert MapSet.member?(read_set, :a) == true
    assert MapSet.member?(read_set, :b) == false
  end

  test "Transaction write set is generated as expected" do
    # test single-op Transactions
    tx1 = Transaction.new(_operations=[Transaction.Op.create(:a, 1)])
    tx2 = Transaction.new(_operations=[Transaction.Op.update(:b, 0)])
    tx3 = Transaction.new(_operations=[Transaction.Op.delete(:c)])
    tx4 = Transaction.new(_operations=[Transaction.Op.read(:a)])

    # check that the read sets are as expected
    assert MapSet.size(tx1.write_set) == 1
    assert MapSet.member?(tx1.write_set, :a) == true
    
    assert MapSet.size(tx2.write_set) == 1
    assert MapSet.member?(tx2.write_set, :b) == true

    assert MapSet.size(tx3.write_set) == 1
    assert MapSet.member?(tx3.write_set, :c) == true

    assert MapSet.size(tx4.write_set) == 0

    # test multi-op Transactions
    # create a Transaction with a couple of operations
    tx = Transaction.new(_operations=[
        Transaction.Op.create(:a, 1),
        Transaction.Op.create(:b, 1),
        Transaction.Op.update(:a, 0),
        Transaction.Op.read(:a),
        Transaction.Op.delete(:a),
    ])
    # check that the write set is as expected
    write_set = tx.write_set

    assert MapSet.size(write_set) == 2
    assert MapSet.member?(write_set, :a) == true
    assert MapSet.member?(write_set, :b) == true
  end

  test "Transaction.Op is_write?/1 works as expected" do
    tx = Transaction.new(_operations=[
      Transaction.Op.read(:z),
      Transaction.Op.create(:a, Transaction.Expression.new(:z, :+, 1)),
      Transaction.Op.update(:z, 0),
    ])

    op1 = Enum.at(tx.operations, 0)
    op2 = Enum.at(tx.operations, 1)
    op3 = Enum.at(tx.operations, 2)

    assert Transaction.Op.is_write?(op1) == false
    assert Transaction.Op.is_write?(op2) == true
    assert Transaction.Op.is_write?(op3) == true
  end
end

defmodule Transaction.ExpressionTest do
  use ExUnit.Case
  doctest Transaction
  doctest Transaction.Op

  test "Transaction.Expression works as expected" do
    tx = Transaction.new(_operations=[
      Transaction.Op.read(:z),
      Transaction.Op.create(:a, Transaction.Expression.new(:z, :+, 1)),
      Transaction.Op.create(:b, 1),
    ])

    op1 = Enum.at(tx.operations, 1)
    op2 = Enum.at(tx.operations, 2)

    evaluated_op1 = Transaction.Op.evaluate_expr(op1, %{z: 1})
    evaluated_op2 = Transaction.Op.evaluate_expr(op2, %{z: 1})

    assert evaluated_op1.expr == 2
    assert evaluated_op2.expr == 1
  end
end