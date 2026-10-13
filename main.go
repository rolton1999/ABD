// Attiya, Bar-Noy, and Dolev
package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// ----------------------------------------------------------------------------

type User interface {
	Write(ctx context.Context, value int) error
	Read(ctx context.Context) (value int, err error)
}

func NewUser(proxyServer Register) User {
	return &userImpl{proxyServer}
}

type userImpl struct {
	proxyServer Register
}

func (c *userImpl) Write(ctx context.Context, value int) error {
	return c.proxyServer.Write(ctx, value)
}

func (c *userImpl) Read(ctx context.Context) (int, error) {
	return c.proxyServer.Read(ctx)
}

// ----------------------------------------------------------------------------

type Register interface {
	Write(ctx context.Context, value int) error
	Read(ctx context.Context) (value int, err error)
}

func NewRegister(nodes ...Node) Register {
	if len(nodes) == 0 {
		panic("trying to setup proxy without nodex")
	}
	return &proxyRegister{atomic.Uint64{}, nodes}
}

// choose node as coordinator
type proxyRegister struct {
	index atomic.Uint64
	nodes []Node
}

func (s *proxyRegister) Write(ctx context.Context, value int) error {
	nodeIndex := s.getNodeIndex()
	return s.nodes[nodeIndex].Write(ctx, value)
}

func (s *proxyRegister) Read(ctx context.Context) (int, error) {
	nodeIndex := s.getNodeIndex()
	return s.nodes[nodeIndex].Read(ctx)
}

func (s *proxyRegister) getNodeIndex() int {
	return int(s.index.Add(1) % uint64(len(s.nodes)))
}

// ----------------------------------------------------------------------------

type Coordinator interface {
	Write(ctx context.Context, value int) error
	Read(ctx context.Context) (int, error)
}

type Node interface {
	Coordinator
	AddReplica(...Node)

	read(ctx context.Context) (StampedValue, error)
	write(ctx context.Context, value StampedValue) error
}

func NewNode(index int) Node {
	return &nodeImpl{
		index:         index,
		others:        nil,
		valuesMutex:   sync.RWMutex{},
		stampedValues: nil,
	}
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

func (n *nodeImpl) AddReplica(others ...Node) {
	n.others = append(n.others, others...)
}

func (n *nodeImpl) Write(ctx context.Context, value int) error {
	// fmt.Printf("start writing on #%d node\n", n.index)

	timestampMutex := sync.Mutex{}
	var timestamp uint64

	var toDos []func(context.Context) bool
	for _, o := range n.others {
		toDos = append(toDos, func(ctx context.Context) bool {
			sv, err := o.read(ctx)
			if err != nil {
				return false
			}

			timestampMutex.Lock()
			defer timestampMutex.Unlock()
			if sv.timestamp > timestamp {
				timestamp = sv.timestamp
			}

			return true
		})
	}

	majority := (len(n.others) + 1) / 2

	// fmt.Printf("start reading phase on #%d node\n", n.index)
	err := DoViaQuorum(
		ctx,
		toDos,
		majority,
	)
	if err != nil {
		return err
	}
	// fmt.Printf("success with reading phase on #%d node\n", n.index)

	toDos = nil
	for _, o := range n.others {
		toDos = append(toDos, func(ctx context.Context) bool {
			err := o.write(ctx, StampedValue{value, timestamp})
			return err == nil
		})
	}

	DoViaQuorum(
		ctx,
		toDos,
		majority,
	)

	return nil
}

func (n *nodeImpl) Read(ctx context.Context) (int, error) {
	mutext := sync.Mutex{}
	var sv StampedValue

	var toDos []func(context.Context) bool
	for _, o := range n.others {
		toDos = append(toDos, func(ctx context.Context) bool {
			readSv, err := o.read(ctx)
			if err != nil {
				return false
			}

			mutext.Lock()
			defer mutext.Unlock()
			if readSv.timestamp >= sv.timestamp {
				sv = readSv
			}

			return true
		})
	}

	majority := (len(n.others) + 1) / 2

	DoViaQuorum(
		ctx,
		toDos,
		majority,
	)

	toDos = nil
	for _, o := range n.others {
		toDos = append(toDos, func(ctx context.Context) bool {
			err := o.write(ctx, sv)
			return err == nil
		})
	}

	DoViaQuorum(
		ctx,
		toDos,
		majority,
	)

	return sv.value, nil
}

func (n *nodeImpl) write(ctx context.Context, sv StampedValue) error {
	// fmt.Printf("attempt to write to #%d node with ts %d\n", n.index, sv.timestamp)
	n.valuesMutex.Lock()
	defer n.valuesMutex.Unlock()

	l := len(n.stampedValues)

	if l > 0 && n.stampedValues[l-1].timestamp > sv.timestamp {
		return fmt.Errorf("keeped timestamp is greater than producing")
	}
	n.stampedValues = append(n.stampedValues, sv)
	return nil
}

func (n *nodeImpl) read(ctx context.Context) (StampedValue, error) {
	// fmt.Printf("attempt to read from #%d node with\n", n.index)
	n.valuesMutex.RLock()
	defer n.valuesMutex.RUnlock()

	if len(n.stampedValues) == 0 {
		return StampedValue{}, nil
	}
	return n.stampedValues[len(n.stampedValues)-1], nil
}

func DoViaQuorum(
	ctx context.Context,
	toDos []func(ctx context.Context) bool,
	majority int,
) error {
	if len(toDos) < majority {
		panic(fmt.Sprintf("majority impossible out of %d", len(toDos)))
	}
	// TODO(make chan unbuffered)
	results := make(chan bool, len(toDos))
	defer close(results)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := sync.WaitGroup{}
	defer wg.Wait()

	for _, doFunc := range toDos {
		wg.Go(func() {
			success := doFunc(ctx)
			select {
			case <-ctx.Done():
				return
			case results <- success:
			}
		})
	}

	for majority > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r := <-results:
			if r {
				majority -= 1
			}
		}
	}

	return nil
}

// ----------------------------------------------------------------------------

func createNodes() []Node {
	node1 := NewNode(0)
	node2 := NewNode(1)
	node3 := NewNode(2)
	node1.AddReplica(node1, node2, node3)
	node2.AddReplica(node1, node2, node3)
	node3.AddReplica(node1, node2, node3)

	return []Node{node1, node2, node3}
}

func createUsers(nodes ...Node) []User {
	return []User{
		getUser(nodes...),
		getUser(nodes...),
		getUser(nodes...),
	}
}

func getUser(nodes ...Node) User {
	register := NewRegister(nodes...)
	return NewUser(register)
}

// ----------------------------------------------------------------------------

func main() {
	nodes := createNodes()

	producers := createUsers(nodes...)
	consumers := createUsers(nodes...)

	ctx := context.Background()

	value := atomic.Int32{}
	wgProducers := sync.WaitGroup{}
	startProduce := make(chan struct{})
	for _, p := range producers {
		wgProducers.Go(func() {
			<-startProduce
			for {
				next := value.Add(1)
				if next > 100 {
					return
				}
				e := p.Write(ctx, int(next))
				fmt.Println("written: ", next)
				_ = e
			}
		})
	}

	wgConsumers := sync.WaitGroup{}
	startConsume := make(chan struct{})
	readValues := [3][]int{}
	for i, c := range consumers {
		wgProducers.Go(func() {
			<-startConsume
			for {
				v, e := c.Read(ctx)
				//fmt.Println(v, e)
				_ = e
				readValues[i] = append(readValues[i], v)
				fmt.Println("read: ", v)
				if v == 100 {
					return
				}
			}
		})
	}
	close(startConsume)
	close(startProduce)
	wgProducers.Wait()
	wgConsumers.Wait()
	for i, vv := range readValues {
		fmt.Println(readValues[i])
		good := true
		for j := 1; j < len(vv); j++ {
			if vv[j-1] > vv[j] {
				good = false
				break
			}
		}
		if !good {
			fmt.Println("broken for ", i)
		}
	}
}
