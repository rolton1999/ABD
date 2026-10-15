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

type StampedValue struct {
	value     int
	timestamp uint64
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
	// fmt.Printf("start writing on #%d node\n", n.index)

	timestampMutex := sync.Mutex{}
	var timestamp uint64

	var reads []func(context.Context) bool
	for _, o := range n.others {
		reads = append(reads, func(ctx context.Context) bool {
			sv, err := o.localRead(ctx)
			if err != nil {
				panic("cannot do any read")
			}

			timestampMutex.Lock()
			timestamp = max(sv.timestamp, timestamp)
			timestampMutex.Unlock()

			return true
		})
	}

	var writes []func(context.Context) bool
	for _, o := range n.others {
		writes = append(writes, func(ctx context.Context) bool {
			err := o.localWrite(ctx, StampedValue{value, timestamp})
			return err == nil
		})
	}

	majority := (len(n.others) + 1) / 2

	for {
		DoViaQuorum(
			ctx,
			reads,
			majority,
		)

		timestamp += 1

		err := DoViaQuorum(
			ctx,
			writes,
			majority,
		)
		if err == nil {
			break
		}
		// fmt.Println("write err: ", err, "gonna retry")
	}
	// fmt.Println("written: ", value)
	return nil
}

func (n *nodeImpl) ABDRead(ctx context.Context) (int, error) {
	mutext := sync.Mutex{}
	var sv StampedValue
	var tsCount int

	var reads []func(context.Context) bool
	for _, o := range n.others {
		reads = append(reads, func(ctx context.Context) bool {
			readSv, err := o.localRead(ctx)
			if err != nil {
				return false
			}

			mutext.Lock()
			if readSv.timestamp >= sv.timestamp {
				sv = readSv
			}
			if readSv.timestamp == sv.timestamp {
				tsCount += 1
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

	majority := (len(n.others) + 1) / 2

	// for {
	DoViaQuorum(
		ctx,
		reads,
		majority,
	)

	// help
	if tsCount != majority {
		DoViaQuorum(
			ctx,
			writes,
			majority,
		)
	}

	// help
	///		if err == nil {
	///			break
	///		}
	///		// fmt.Println("read err: ", err, "gonna retry")
	///	}

	return sv.value, nil
}

func (n *nodeImpl) localWrite(ctx context.Context, sv StampedValue) error {
	n.valuesMutex.Lock()
	defer n.valuesMutex.Unlock()

	l := len(n.stampedValues)

	if l > 0 {
		last := n.stampedValues[l-1]
		if last.timestamp > sv.timestamp {
			return fmt.Errorf("keeped timestamp is greater than producing")
		} else if last.timestamp == sv.timestamp {
			if last.value != sv.value {
				return fmt.Errorf("keeped timestamp is equal but value differs")
			}
			return nil
		} else if last.value == sv.value {
			n.stampedValues[l-1].timestamp = sv.timestamp
			return nil
		}
	}

	n.stampedValues = append(n.stampedValues, sv)
	return nil
}

func (n *nodeImpl) localRead(ctx context.Context) (StampedValue, error) {
	// fmt.Printf("attempt to read from #%d node with\n", n.index)
	n.valuesMutex.RLock()
	defer n.valuesMutex.RUnlock()

	if len(n.stampedValues) == 0 {
		return StampedValue{}, nil
	}
	return n.stampedValues[len(n.stampedValues)-1], nil
}

func (n *nodeImpl) dumpSeenValues() []int {
	// fmt.Println(n.stampedValues)
	var ans []int
	for _, v := range n.stampedValues {
		ans = append(ans, v.value)
	}
	return ans
}
