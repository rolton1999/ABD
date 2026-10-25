package cluster

type Timestamp struct {
	counter               uint64
	coordinatorTieBreaker uint64
}

func (t *Timestamp) Greater(other Timestamp) bool {
	if t.counter == other.counter {
		return t.coordinatorTieBreaker > other.coordinatorTieBreaker
	}
	return t.counter > other.counter
}

func (t *Timestamp) Inc(coordinatorTieBreaker uint64) {
	t.coordinatorTieBreaker = coordinatorTieBreaker
	t.counter += 1
}

type StampedValue struct {
	Value     int
	Timestamp Timestamp
}
