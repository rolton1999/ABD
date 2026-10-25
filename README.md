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

The observer records operation kind, input or returned value, and invocation and
response event numbers from one shared atomic counter. These event numbers are
separate from ABD tags. If A finishes before B starts, A must precede B in the
candidate ordering. Overlapping operations may appear in either order. Starting
with value `0`, every read must return the latest preceding write's value.
Backtracking and memoization search for one ordering containing every operation
exactly once.

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
