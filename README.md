# Calvin deterministic distributed transaction layer in Elixir

`@author andrii dobroshynskyi`

## Intro

I am interested in topics in OLTP systems and specifically in distributed transactions, so for the project I would like to implement a system called [Calvin](http://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf) proposed in the paper from 2012 that is meant to be a transaction scheduling and replication layer that can be used on a non-transactional storage system to turn it into a system with support for full ACID transactions.

Calvin comprises of 3 main components that act as "layers" - sequencer, scheduler, and the storage, with the storage engine being able to be "plugged-in" as desired. The sequencer is what reads transactional inputs and takes care of replicating this globally ordered list. The scheduler is the part that executes the transactions in order specified by the sequencer in a deterministic manner with what essentially is two-phase locking but a bit modified. 

The paper suggests that the system can use both async replication and Paxos based replication, so my plan is to start with the async case to get it working.

I suggest that I will first implement a very trivial key-value store supporting CRUD operations for storage and then build functionality on top of that. I also plan to get it working assuming a single-node deployment and then add the extensions to see what scalability we can achieve.

If the implementation goes as planned, time-permitting I would be interested in adding a feature suggested by the Calvin paper to implement a monitoring component to orchistrate failovers and monitor load. 

## Implementation plan

I intend to use Elixir for the project since I think it may be helpful to use the Emulation functionality and it's been nice to work with Elixir. I plan to implement first assuming theres is only one node running and then extend to multiple nodes. A tentative outline plan that I have drafted is as follows:

- Implement basic in-memory key-value store in Elixir
- Implement the sequencer to take transactional input from clients and generate a sequence of txns to execute in order (paper suggests that this is done in 10ms epochs)
- Implement the replication component of sequencer to communicate the input across all nodes (initially skip since assuming only one node running)
- Implement the scheduler that will fire off transaction execution threads and use 2PL with modifications to execute the transactions. 
- Implement the functionality to recover in case of abort (Calvin requires the node to recover either from a fellow node or by replaying the inputs provided by the sequencer)
- Run micro-benchmarks to see how compares to the results in paper
- Time-permitting implement an extension

## Material / References

Some of the papers I have consulted so far that I will be using to implement parts of project.

- Calvin Paper - [Thomson et al. '12](http://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf)
- Different print of Calvin paper - [Fast Distributed Transactions and Strongly Consistent Replication for OLTP Database Systems](http://www.cs.umd.edu/~abadi/papers/calvin-tods14.pdf)
- Paper preceding Calvin by same authors - [Thomson, Abadi VLDB '10](http://www.cs.umd.edu/~abadi/papers/determinism-vldb10.pdf)
- Paper with detailed analysis of the Calvin deterministic db system pros/cons implemented by same authors - [Ren, Thomson, Abadi VLDB '14](http://www.vldb.org/pvldb/vol7/p821-ren.pdf)
- Reading on transactions, recovery - [Concurrency Control and Recovery](https://dsf.berkeley.edu/cs262/2005/ccandr.pdf)
- Reading on atomicity, locking - [Principles of Computer System Design, Chapter 9](https://ocw.mit.edu/resources/res-6-004-principles-of-computer-system-design-an-introduction-spring-2009/online-textbook/atomicity_open_5_0.pdf)