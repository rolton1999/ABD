package cluster

type Timestamp struct {
	counter           uint64
	coordinatorNodeID int
}

func (t *Timestamp) Greater(other Timestamp) bool {
	if t.counter == other.counter {
		return t.coordinatorNodeID > other.coordinatorNodeID
	}
	return t.counter > other.counter
}

func (t *Timestamp) Inc(nodeID int) {
	t.coordinatorNodeID = nodeID
	t.counter += 1
}

type StampedValue struct {
	Value     int
	Timestamp Timestamp
}
