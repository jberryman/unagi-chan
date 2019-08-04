[![Build Status](https://travis-ci.com/jberryman/unagi-chan.svg?branch=master)](https://travis-ci.com/jberryman/unagi-chan)

The library is [available on hackage](http://hackage.haskell.org/package/unagi-chan)
and you can install it with:

    $ cabal install unagi-chan

## Design

The idea is to design a queue around the x86 fetch-and-add instruction, which
performs well under contention.

The queue is conceptually simple, consisting of: an infinite array, and two
atomic counters, one for readers and another for writers. A read or write
operation consists of incrementing the appropriate counter and racing to
perform an atomic operation on the specified index. 

If the writer wins it has written its value for the reader to find and exits.
When it loses it does a rendezvous with the blocked or blocking reader, via
another mechanism and hands off its value.

## Linearizabillity

The queue has FIFO semantics, reasoning in terms of linearizability. Our atomic
counter ensures that all non-overlapping reads and writes are assigned indices
in temporal order.

## Lockfree - ness

Operations are non-blocking, with the exception that a stalled writer may block
at most one reader (the reader "assigned" to it by our internal counter).

## Performance

Here is an example benchmark measuring the time taken to concurrently write and
read 100,000 messages, with work divided amongst increasing number of readers
and writers, comparing against the top-performing queues in the standard
libraries. The inset graph shows a zoomed-in view on the implementations here.

![Benchmarks](http://i.imgur.com/J5rLUFn.png)

Some of these variants may be deprecated in the future if they are found to
provide little performance benefit, or no unique features; you should benchmark
and experiment with them for your use cases, and please submit pull requests
for additions to the benchmark suite that reflect what you find.
