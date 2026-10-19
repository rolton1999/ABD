package main

import (
	"context"
	"fmt"
	"sync"
)

type Node interface {
	ABDWrite(ctx context.Context, value int) error
	ABDRead(ctx context.Context) (int, error)

	addReplicas(...Node)

	localRead(ctx context.Context) (StampedValue, error)
	localWrite(ctx context.Context, value StampedValue) error

	dumpSeenValues() []int
}

func NewNode(index int) Node {
	return &nodeImpl{
		index:         index,
		others:        nil,
		valuesMutex:   sync.RWMutex{},
		stampedValues: nil,
	}
}

func NewNodes(n int) []Node {
	var nodes []Node
	for i := range n {
		nodes = append(nodes, NewNode(i))
	}
	for i := range n {
		nodes[i].addReplicas(nodes...)
	}
	return nodes
}

type Timestamp struct {
	timestamp         uint64
	coordinatorNodeID int
}

func (t *Timestamp) Greater(other Timestamp) bool {
	if t.timestamp == other.timestamp {
		return t.coordinatorNodeID > other.coordinatorNodeID
	}
	return t.timestamp > other.timestamp
}

func (t *Timestamp) Inc(nodeID int) {
	t.coordinatorNodeID = nodeID
	t.timestamp += 1
}

type StampedValue struct {
	value     int
	timestamp Timestamp
}

type nodeImpl struct {
	index  int
	others []Node

	valuesMutex   sync.RWMutex
	stampedValues []StampedValue
}

func (n *nodeImpl) addReplicas(others ...Node) {
	n.others = append(n.others, others...)
}

func (n *nodeImpl) ABDWrite(ctx context.Context, value int) error {
	timestampMutex := sync.Mutex{}
	var timestamp Timestamp

	var reads []func(context.Context) bool
	for _, o := range n.others {
		reads = append(reads, func(ctx context.Context) bool {
			sv, err := o.localRead(ctx)
			if err != nil {
				panic("cannot do any read")
			}

			timestampMutex.Lock()
			if sv.timestamp.Greater(timestamp) {
				timestamp = sv.timestamp
			}
			timestampMutex.Unlock()

			return true
		})
	}

	majority := len(n.others)/2 + 1

	for {
		DoViaQuorum(
			ctx,
			reads,
			majority,
		)

		timestamp.Inc(n.index)

		// to capture the new incremented timestamp value
		var writes []func(context.Context) bool
		for _, o := range n.others {
			writes = append(writes, func(ctx context.Context) bool {
				err := o.localWrite(ctx, StampedValue{value, timestamp})
				return err == nil
			})
		}

		err := DoViaQuorum(
			ctx,
			writes,
			majority,
		)
		if err == nil {
			break
		}
	}

	return nil
}

func (n *nodeImpl) ABDRead(ctx context.Context) (int, error) {
	mutext := sync.Mutex{}
	var sv StampedValue

	var reads []func(context.Context) bool
	for _, o := range n.others {
		reads = append(reads, func(ctx context.Context) bool {
			readSv, err := o.localRead(ctx)
			if err != nil {
				return false
			}

			mutext.Lock()
			if readSv.timestamp.Greater(sv.timestamp) {
				sv = readSv
			}
			mutext.Unlock()

			return true
		})
	}

	var writes []func(context.Context) bool
	for _, o := range n.others {
		writes = append(writes, func(ctx context.Context) bool {
			err := o.localWrite(ctx, sv)
			return err == nil
		})
	}

	majority := len(n.others)/2 + 1

	for {
		DoViaQuorum(
			ctx,
			reads,
			majority,
		)

		// helping
		err := DoViaQuorum(
			ctx,
			writes,
			majority,
		)
		if err == nil {
			break
		}
	}

	return sv.value, nil
}

func (n *nodeImpl) localWrite(ctx context.Context, sv StampedValue) error {
	n.valuesMutex.Lock()
	defer n.valuesMutex.Unlock()

	l := len(n.stampedValues)

	if l > 0 {
		last := n.stampedValues[l-1]
		if sv.timestamp == last.timestamp && sv.value == last.value {
			return nil
		}
		if sv.timestamp == last.timestamp && sv.value == last.value {
			return fmt.Errorf("collistion of values on the same timestamp")
		}
		if !sv.timestamp.Greater(last.timestamp) {
			return fmt.Errorf("keeped timestamp is greater or eq than producing")
		}
	}

	n.stampedValues = append(n.stampedValues, sv)
	return nil
}

func (n *nodeImpl) localRead(ctx context.Context) (StampedValue, error) {
	n.valuesMutex.RLock()
	defer n.valuesMutex.RUnlock()

	if len(n.stampedValues) == 0 {
		return StampedValue{}, nil
	}
	return n.stampedValues[len(n.stampedValues)-1], nil
}

func (n *nodeImpl) dumpSeenValues() []int {
	var ans []int
	for _, v := range n.stampedValues {
		ans = append(ans, v.value)
	}
	return ans
}
