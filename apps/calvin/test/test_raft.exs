defmodule RaftTest do
  use ExUnit.Case

  doctest Raft

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Raft.Log and Log.Entry work as expected" do
    # create a new Raft log
    log = Raft.Log.new()
    assert log.entries == []

    # create an empty log entry
    entry = Raft.Log.Entry.empty()
    assert entry.index == 0 && entry.term == 0 && entry.batch == []

    # create a log entry with some Transactions
    entry = Raft.Log.Entry.new(_index=1, _term=1, _batch=[
        Transaction.new(_operations=[
          Transaction.Op.create(:a, 1),
          Transaction.Op.create(:b, 1)    
        ]),
        Transaction.new(_operations=[
          Transaction.Op.update(:a, 0)
        ])
      ]
    )
    assert entry.index == 1 && entry.term == 1 && length(entry.batch) == 2
  end

  test "Raft.Log utility functions work as expected" do
    # test with an empty log
    log = Raft.Log.new()

    assert Raft.Log.get_last_log_index(log) == 0
    assert Raft.Log.get_last_log_term(log) == 0
    assert Raft.Log.logged?(log, 1) == false

    assert Raft.Log.get_entry_at_index(log, 1) == :noentry
    assert Raft.Log.get_entries_at_index(log, 1) == []
    assert Raft.Log.remove_entries(log, _from_index=1).entries == []

    # add some log entries to the log and test the utility functions
    log = Raft.Log.add_entries(log, _entries=[
      Raft.Log.Entry.new(_index=1, _term=1, _batch=[
        Transaction.new(_operations=[
          Transaction.Op.create(:a, 1)
        ]),
      ]), 
      Raft.Log.Entry.new(_index=2, _term=1, _batch=[
        Transaction.new(_operations=[
          Transaction.Op.create(:b, 1)
        ])
      ])
    ])
    assert length(log.entries) == 2

    assert Raft.Log.get_last_log_index(log) == 2
    assert Raft.Log.get_last_log_term(log) == 1
    assert Raft.Log.logged?(log, 2) == true
    assert Raft.Log.logged?(log, 3) == false

    assert length(Raft.Log.get_entry_at_index(log, 1).batch) == 1
    assert length(Raft.Log.get_entries_at_index(log, 1)) == 2
    assert length(Raft.Log.get_entries_at_index(log, 2)) == 1
    assert Raft.Log.get_entry_at_index(log, 3) == :noentry

    assert length(Raft.Log.remove_entries(log, _from_index=2).entries) == 1
  end
end


