# ABD replicated register

I mplementing an in-memory, multiwriter variant of the Attiya–Bar-Noy–Dolev 
(ABD) atomic register algorithm. Goroutines model concurrent
clients and replica requests. The experiment assumes all replicas respond and
operations use a background context; it does not simulate network or replica
failures.

Original paper: [Sharing Memory Robustly in Message-Passing Systems
(PODC 1990)](https://groups.csail.mit.edu/tds/papers/Attiya/PODC90.pdf).
The original construction is single-writer; this project explores concurrent
writers using a query phase and unique, totally ordered tags.

## Run

Use Go 1.27.1 or later, as declared in `go.mod`. No external dependencies are
required.

```sh
go run .
```

The default workload runs 1,000 independent experiments with three replicas,
two writers, two readers, and ten writes per experiment. Readers run until the
writers finish. Change the arguments in `main.go` to explore other workloads;
there are no command-line flags.

## Protocol

The register initially contains `0`. Each stored value has a tag consisting of a
counter, a per-coordinator allocation number, and a coordinator ID. Larger
counters take precedence; for equal counters, smaller allocation numbers and
then smaller coordinator IDs take precedence. These fields distinguish concurrent
writes within the current process, assuming no overflow.

Tag uniqueness follows from allocation as well as the `Timestamp` structure:
`ABDWrite` obtains its allocation number with the coordinator's atomic
`timestampTieBraker.Add(1)`. Concurrent writes through the same coordinator get
different allocation numbers; writes through different coordinators have
different coordinator IDs. Thus distinct writes cannot have identical tags
within one cluster lifetime, assuming neither counter wraps and coordinators
are not restarted with reset allocation state. Incrementing the maximum
observed counter makes the new tag greater than every tag observed by the query,
regardless of the ordering of the secondary fields.

A write queries replicas, selects the maximum observed tag, increments its
counter, and assigns a unique tag to the logical write. It propagates that same
tagged value and waits for majority acknowledgments. It does not publish fresh
tags through write retries.

A read queries replicas, selects the maximum tagged value, and propagates that
same pair in a repair phase. It waits for majority acknowledgments before
returning the selected value.

A replica stores an incoming pair only if its tag is greater than the stored
tag. Otherwise it retains its newer state and still acknowledges the request.
An acknowledgment therefore establishes that the replica holds a tag **at least
as large as the requested tag**, not necessarily the exact requested value.

Consequently, a read may return a value that was never stored on a majority.
Concurrent writes can supersede it during repair. The quorum guarantee is that
before the read returns, a majority holds tags at least as large as its selected
tag. Subsequent non-overlapping reads cannot go backward in tag order.

## Validation

The executable checks two properties:

- Each individual reader observes nondecreasing tags across its sequential reads.
- The observer searches for a legal sequential ordering of a small recorded
  history that preserves real-time precedence.

The second check is a **linearizability check**. A candidate ordering must
contain every completed operation exactly once, with its actual input and
result. It is legal when each read returns the latest preceding write's value,
or the initial `0` if no write precedes it. It preserves real-time precedence
when an operation that finishes before another starts also appears before it
in the ordering. Overlapping operations may be ordered either way, provided
the entire sequence remains legal.

Finding such an ordering establishes that the recorded history is
**linearizable**. The ordering itself is **a linearization** of that history.

The monotonicity assertion in `main.go` checks **reader observations**, not the
internal history of each replica. It compares consecutive returned tags for
each client whose reads are sequential. Replica-state monotonicity is enforced
separately by `localWrite` in `cluster/node.go`: while holding the replica's
mutex, it appends a new pair only when its tag is strictly greater than the
current tag. Equal or older requests leave state unchanged. There is currently
no separate assertion that scans replica tag histories in `main.go`.

Goroutines and mutex-protected replica operations provide a model in which the
ABD safety argument can be applied: scheduling supplies request interleavings,
and each local replica operation is atomic. Unique write tags and monotonic
replica state are implementation invariants supporting that argument; the
history checker independently tests observable register behavior on recorded
executions.

The observer records operation kind, input or returned value, and invocation and
response event numbers from one shared atomic counter. These event numbers are
separate from ABD tags. If A finishes before B starts, A must precede B in the
candidate ordering. Overlapping operations may appear in either order. Starting
with value `0`, every read must return the latest preceding write's value.
Backtracking and memoization search for one ordering containing every operation
exactly once.

### Backtracking and pruning

The checker builds a candidate sequence one operation at a time, carrying the
set of placed operations and the current register value. A write changes that
value; a read must match it. If a choice cannot lead to a complete legal
sequence, the search removes that choice and tries another. One complete valid
sequence is enough for success; it does not need to enumerate all linearizations.

The search prunes branches in three ways:

- **Read results:** a read returning a value different from the current register
  value cannot be selected next.
- **Real-time precedence:** after selecting an operation, the recursive call
  rejects it if an unplaced operation finished before it started. This prevents
  a candidate sequence from reversing required real-time order.
- **Memoization:** after precedence validation, the checker records the placed
  operation mask and current register value. Different valid prefixes reaching
  the same state have the same remaining operations and possible continuations,
  so the search need not explore that state twice. The key represents a search
  state, not the full order of the prefix.

Sorting by invocation time alone cannot replace this search: overlapping
operations may need to take effect in the opposite order from their starts.
Checking each read independently is also insufficient; every read and write
must fit into one common sequential history.

Pruning reduces work but does not guarantee a cheap search. Without pruning,
there can be up to `n!` candidate operation orders. With exact state
memoization, many prefixes merge, but the number of placed-operation subsets
can still grow exponentially. Extensive overlap creates choices; real-time
constraints and read results eliminate choices. Runtime therefore depends on
the history's structure as well as its length. The current checker has a size
limit but no timeout, and even a history below the limit can be expensive.

The initial-call loop tries each operation index with a fresh cache and empty
placed set. This compensates for the initial precedence check, but repeats
search work rather than explicitly fixing a different first operation.

For this checker, an exhaustive search returning `false` means no legal ordering
was found under its model and supported key range. Skipping a history means it
was not checked. Finding a valid ordering checks only that recorded execution;
it does not explore all possible goroutine schedules. These conclusions also
depend on correct history recording and a correct checker implementation.

Current checker limits:

- Histories longer than 32 operations are skipped. The temporary bypass returns
  success, so `main` also prints a success message for skipped histories. Read
  the preceding output to distinguish these from checked histories. The read
  count is unbounded even when the write count is small.
- The packed cache key retains only 32 bits of the register value. It suits the
  current small positive workload but can collide for arbitrary Go `int` values.
- Recorded operations are assumed successful; uncertain outcomes are not modeled.

Passing these experiments is evidence about recorded executions, not a proof of
correctness for every possible schedule.

This is a concurrency experiment rather than a production storage system.
Replica state and diagnostic histories live in memory. The quorum helper waits
for every worker to finish, even after obtaining a majority. Write quorum errors
are currently ignored, and reads retain a retry loop; cancellation and failure
handling are outside the current experiment's assumptions.
