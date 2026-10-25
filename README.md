# ABD Replicated Register

An in-memory Go implementation of a multiwriter variant of the
Attiya–Bar-Noy–Dolev (ABD) atomic register algorithm, with concurrent workloads
and a linearizability checker.

Goroutines model clients and replica requests. The implementation explores
quorum replication and concurrent reads and writes under the assumption that
all replicas respond successfully.

## Run

Requires Go 1.27.1 or later. No external dependencies.

```sh
go run .
```

The default workload runs 1,000 independent experiments, each with three
replicas, two writers, two readers, and ten writes. Readers continue until the
writers finish. Configure the workload in `main.go`.

## Protocol

The register starts at `0`. Values carry unique, totally ordered tags composed
of a counter, a per-coordinator allocation number, and a coordinator ID.

- **Write:** query replicas, increment the highest observed counter, assign a
  unique tag, then propagate the tagged value and obtain majority acknowledgments.
- **Read:** query replicas, select the highest tagged value, then write that
  same pair back and obtain majority acknowledgments before returning it.
- **Replica:** accept only newer tags; acknowledge equal or older requests
  without changing state.

An acknowledgment guarantees a tag at least as large as the requested tag.
Concurrent writes may supersede a read's selected value during write-back;
subsequent non-overlapping reads cannot return an older tag.

## Validation

Each experiment checks that individual readers observe nondecreasing tags.
The observer also records operation inputs, results, and invocation/response
order, then uses backtracking with memoization to search for a legal sequential
history that preserves real-time precedence.

The checker has practical limits:

- Histories over 32 operations are skipped. The program still prints a success
  message; `History is too big to check` identifies skipped histories.
- Search can take exponential time and has no timeout.
- The memoization key retains only 32 bits of the register value, so arbitrary
  Go `int` values can collide. The default workload uses small positive values.

Passing a check validates the recorded execution, not every possible schedule.

## Scope

This project is a concurrency experiment. Replica state and diagnostic
histories are held in memory; network and replica failures are not simulated.
The quorum helper waits for all workers to finish even after reaching a
majority. Quorum error handling and cancellation remain incomplete, and the
observer assumes successful operations.

## Reference

[Attiya, Bar-Noy, and Dolev: Sharing Memory Robustly in Message-Passing Systems
(PODC 1990)](https://groups.csail.mit.edu/tds/papers/Attiya/PODC90.pdf).
The original construction is single-writer; this implementation extends the
approach to concurrent writers.
