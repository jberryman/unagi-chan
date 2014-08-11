The library is [available on hackage](http://hackage.haskell.org/package/unagi-chan)
and you can install it with:

    $ cabal install unagi-chan

## Design

The idea is to design a queue around the x86 fetch-and-add instruction, which
performs well under contention.

The queue is conceptually simple, consisting of: an infitinite array, and two
atomic counters, one for readers and another for writers. A read or write
operation consists of incrementing the appropriate counter and racing to
perform an atomic operation on the specified index. 

If the writer wins it has written its value for the reader to find and exits.
When it loses it does a rendevous with the blocked or blocking reader, via
another mechanism and hands off its value.

## Linearizabillity

The queue has FIFO semantics, reasoning in terms of linearizability. Our atomic
counter ensures that all non-overlapping reads and writes are assigned indices
in temporal order.

## Lockfree - ness

Operations are non-blocking, with the exception that a stalled writer may block
at most one reader (the reader "assigned" to it by our internal counter).
