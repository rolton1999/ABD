// Package cluster: Attiya, Bar-Noy, and Dolev
package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type Coordinator interface {
	ABDWrite(ctx context.Context, value int) error
	ABDRead(ctx context.Context) (StampedValue, error)

	DumpActions() []int
}

type replica interface {
	localRead(ctx context.Context) (StampedValue, error)
	localWrite(ctx context.Context, value StampedValue) error
}

type node interface {
	Coordinator
	replica

	addReplicas(...node)
}

func newNode(index uint64) node {
	return &nodeImpl{
		index:              index,
		others:             nil,
		timestampTieBraker: atomic.Uint64{},
		valuesMutex:        sync.RWMutex{},
		stampedValues:      nil,
	}
}

func NewCluster(n int) []Coordinator {
	nodes := make([]node, 0, n)
	for i := range n {
		nodes = append(nodes, newNode(uint64(i)))
	}
	for i := range n {
		nodes[i].addReplicas(nodes...)
	}

	coordinators := make([]Coordinator, n)
	for i := range n {
		coordinators[i] = nodes[i].(Coordinator)
	}

	return coordinators
}

type nodeImpl struct {
	index  uint64
	others []node

	timestampTieBraker atomic.Uint64

	valuesMutex   sync.RWMutex
	stampedValues []StampedValue
}

func (n *nodeImpl) addReplicas(others ...node) {
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
			if sv.Timestamp.Greater(timestamp) {
				timestamp = sv.Timestamp
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

		// same result as with n.index as tiebreaker
		// timestamp.Inc(0)

		// 30 producers, 30 consumers, 10kk vals: producers done in 1m6.532859625s
		// 2 procuders, 2 consumers, 10 vals: producers done in 244.416µs
		// timestamp.Inc(uint64(n.index))

		// 30 producers, 30 consumers, 10kk vals: producers done in 58.474119084s
		// 2 procuders, 2 consumers, 10 vals: producers done in 162.167µs
		timestamp.Inc(n.timestampTieBraker.Add(1), n.index)

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

func (n *nodeImpl) ABDRead(ctx context.Context) (StampedValue, error) {
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
			if readSv.Timestamp.Greater(sv.Timestamp) {
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

	return sv, nil
}

func (n *nodeImpl) localWrite(ctx context.Context, sv StampedValue) error {
	n.valuesMutex.Lock()
	defer n.valuesMutex.Unlock()

	l := len(n.stampedValues)

	if l > 0 {
		last := n.stampedValues[l-1]
		if sv.Timestamp == last.Timestamp && sv.Value == last.Value {
			return nil
		}
		if !sv.Timestamp.Greater(last.Timestamp) {
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

func (n *nodeImpl) DumpActions() []int {
	var ans []int
	for _, v := range n.stampedValues {
		ans = append(ans, v.Value)
	}
	return ans
}
