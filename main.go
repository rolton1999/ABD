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

	var reads []func(context.Context) bool
	for _, o := range n.others {
		reads = append(reads, func(ctx context.Context) bool {
			sv, err := o.read(ctx)
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
			err := o.write(ctx, StampedValue{value, timestamp})
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

func (n *nodeImpl) Read(ctx context.Context) (int, error) {
	mutext := sync.Mutex{}
	var sv StampedValue
	var tsCount int

	var reads []func(context.Context) bool
	for _, o := range n.others {
		reads = append(reads, func(ctx context.Context) bool {
			readSv, err := o.read(ctx)
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
			err := o.write(ctx, sv)
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

func (n *nodeImpl) write(ctx context.Context, sv StampedValue) error {
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

func (n *nodeImpl) read(ctx context.Context) (StampedValue, error) {
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
	// defer close(results)

	wg := sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, doFunc := range toDos {
		wg.Go(func() {
			success := doFunc(ctx)
			select {
			case <-ctx.Done():
				// fmt.Println("gone by conext done")
				return
			case results <- success:
			}
			// fmt.Println("sucess written")
		})
	}

	failed := 0
	for majority > 0 && failed+majority != len(toDos) {
		// fmt.Println("maj, f: ", majority, failed)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r := <-results:
			if r {
				majority -= 1
			} else {
				failed += 1
			}
		}
	}
	if failed == len(toDos) || majority > 0 {
		return fmt.Errorf("failed on quorum")
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

func unify(d []int) []int {
	j := 0
	for i := range d {
		if d[i] != d[j] {
			j += 1
			d[j] = d[i]
		}
	}
	return d[:j+1]
}

// ----------------------------------------------------------------------------

func main() {
	nodes := createNodes()

	producers := createUsers(nodes...)

	ctx := context.Background()

	const shouldBeWritten = 100

	value := atomic.Int32{}

	wgProducers := sync.WaitGroup{}
	startProduce := make(chan struct{})

	for i, p := range producers {
		wgProducers.Go(func() {
			<-startProduce
			for {
				next := value.Add(1)
				if next > shouldBeWritten {
					break
				}
				e := p.Write(ctx, int(next))
				if e != nil {
					panic("cannot write")
				}
			}
			fmt.Println(i, "writing done!")
		})
	}

	consumers := createUsers(nodes...)
	wgConsumers := sync.WaitGroup{}
	startConsume := make(chan struct{})
	stopConsume := atomic.Bool{}
	readValues := [3][]int{}
	for i, c := range consumers {
		wgConsumers.Go(func() {
			<-startConsume
			for {
				v, e := c.Read(ctx)
				_ = e
				//fmt.Println(v, e)
				readValues[i] = append(readValues[i], v)
				if stopConsume.Load() {
					break
				}
			}
		})
	}

	close(startConsume)
	close(startProduce)

	wgProducers.Wait()
	stopConsume.Store(true)
	wgConsumers.Wait()

	fmt.Println("from nodes:")
	for i := range nodes {
		fmt.Println(nodes[i].dumpSeenValues())
	}

	fmt.Println("readers seen:")
	for i := range readValues {
		readValues[i] = unify(readValues[i])
		fmt.Println(readValues[i])
	}

	fmt.Println("check if every value was on nodes once")
	for i, v := range nodes {
		h := map[int]bool{}
		for _, vv := range v.dumpSeenValues() {
			if h[vv] {
				fmt.Printf("node #%d broken, has two or more %d\n", i, vv)
			}
			h[vv] = true
		}
	}
}
