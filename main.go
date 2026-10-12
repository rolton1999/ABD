// Attiya, Bar-Noy, and Dolev
package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// ----------------------------------------------------------------------------

type Client interface {
	Write(ctx context.Context, key string, value string) error
	Read(ctx context.Context, key string) (value string, err error)
}

func NewClient(proxyServer Server) Client {
	return &clientImpl{proxyServer}
}

type clientImpl struct {
	proxyServer Server
}

func (c *clientImpl) Write(ctx context.Context, key string, value string) error {
	return c.proxyServer.ProcessWrite(ctx, key, value)
}

func (c *clientImpl) Read(ctx context.Context, key string) (string, error) {
	return c.proxyServer.ProcessRead(ctx, key)
}

// ----------------------------------------------------------------------------

type Server interface {
	ProcessWrite(ctx context.Context, key string, value string) error
	ProcessRead(ctx context.Context, key string) (value string, err error)
}

func NewProxyServer(nodes ...Coordinator) Server {
	if len(nodes) == 0 {
		panic("trying to setup proxy without nodex")
	}
	return &proxyServer{atomic.Uint64{}, nodes}
}

// choose node as coordinator
type proxyServer struct {
	index atomic.Uint64
	nodes []Coordinator
}

func (s *proxyServer) ProcessWrite(ctx context.Context, key, value string) error {
	nodeIndex := s.getNodeIndex()
	return s.nodes[nodeIndex].Write(ctx, key, value)
}

func (s *proxyServer) ProcessRead(ctx context.Context, key string) (string, error) {
	nodeIndex := s.getNodeIndex()
	return s.nodes[nodeIndex].Read(ctx, key)
}

func (s *proxyServer) getNodeIndex() int {
	return int(s.index.Add(1) % uint64(len(s.nodes)))
}

// ----------------------------------------------------------------------------

type Coordinator interface {
	Write(ctx context.Context, key string, value string) error
	Read(ctx context.Context, key string) (string, error)
}

type Node interface {
	Coordinator
	AddNode(...Node)

	read(ctx context.Context, key string) (StampedValue, error)
	write(ctx context.Context, key string, value StampedValue) error
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
	value     string
	timestamp uint64
}

type nodeImpl struct {
	index  int
	others []Node

	valuesMutex   sync.RWMutex
	stampedValues []StampedValue
}

func (n *nodeImpl) AddNode(others ...Node) {
	n.others = append(n.others, others...)
}

func (n *nodeImpl) Write(ctx context.Context, key string, value string) error {
	fmt.Printf("start writing on #%d node\n", n.index)

	timestampMutex := sync.Mutex{}
	var timestamp uint64

	var toDos []func(context.Context) bool
	for _, o := range n.others {
		toDos = append(toDos, func(ctx context.Context) bool {
			sv, err := o.read(ctx, key)
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

	fmt.Printf("start reading phase on #%d node\n", n.index)
	err := DoViaQuorum(
		ctx,
		toDos,
		majority,
	)
	if err != nil {
		return err
	}
	fmt.Printf("success with reading phase on #%d node\n", n.index)

	toDos = nil
	for _, o := range n.others {
		toDos = append(toDos, func(ctx context.Context) bool {
			err := o.write(ctx, key, StampedValue{value, timestamp})
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

func (n *nodeImpl) Read(ctx context.Context, key string) (string, error) {
	fmt.Printf("attempt to read from #%d node\n", n.index)
	return "", nil
}

func (n *nodeImpl) write(ctx context.Context, key string, sv StampedValue) error {
	fmt.Printf("attempt to write to #%d node with ts %d\n", n.index, sv.timestamp)
	return nil
}

func (n *nodeImpl) read(ctx context.Context, key string) (StampedValue, error) {
	fmt.Printf("attempt to read from #%d node with\n", n.index)
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
			fmt.Println("doFunc ended")
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

func main() {
	node1 := NewNode(0)
	node2 := NewNode(1)
	node3 := NewNode(2)
	node1.AddNode(node1, node2, node3)
	node2.AddNode(node1, node2, node3)
	node3.AddNode(node1, node2, node3)

	proxyServer := NewProxyServer(node1, node2, node3)
	client := NewClient(proxyServer)

	ctx := context.Background()
	client.Write(ctx, "key", "value")
}
