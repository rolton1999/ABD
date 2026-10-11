// Attiya, Bar-Noy, and Dolev
package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
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

func NewProxyServer(nodes ...Server) Server {
	if len(nodes) == 0 {
		panic("trying to setup proxy without nodex")
	}
	return &proxyServer{atomic.Uint64{}, nodes}
}

// choose node as coordinator
type proxyServer struct {
	index atomic.Uint64
	nodes []Server
}

func (s *proxyServer) ProcessWrite(ctx context.Context, key, value string) error {
	nodeIndex := s.getNodeIndex()
	return s.nodes[nodeIndex].ProcessWrite(ctx, key, value)
}

func (s *proxyServer) ProcessRead(ctx context.Context, key string) (string, error) {
	nodeIndex := s.getNodeIndex()
	return s.nodes[nodeIndex].ProcessRead(ctx, key)
}

func (s *proxyServer) getNodeIndex() int {
	return int(s.index.Add(1) % uint64(len(s.nodes)))
}

// ----------------------------------------------------------------------------

type Node interface {
	Server
	AddNode(...Node)
}

func NewNode() Node {
	return &nodeImpl{}
}

type StampedValue struct {
	value     string
	timestamp uint64
}

type nodeImpl struct {
	others        []Node
	stampedValues []StampedValue
}

func (n *nodeImpl) AddNode(others ...Node) {
	n.others = append(n.others, others...)
}

func (n *nodeImpl) Write(ctx context.Context, key string, value string) error {
	return nil
}

func (n *nodeImpl) Read(ctx context.Context, key string) (string, error) {
	return "", nil
}

func (n *nodeImpl) ProcessWrite(ctx context.Context, key string, value string) error {
	return nil
}

func (n *nodeImpl) ProcessRead(ctx context.Context, key string) (string, error) {
	return "", nil
}

func ViaQuorum(
	ctx context.Context,
	toDos []func(ctx context.Context) bool,
	majority int,
	timeout time.Duration,
) error {
	results := make(chan bool)
	defer close(results)

	ctx, cancel := context.WithTimeout(ctx, timeout)
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

func main() {
	node1 := NewNode()
	node2 := NewNode()
	node3 := NewNode()
	node1.AddNode(node1, node2, node3)
	node2.AddNode(node1, node2, node3)
	node3.AddNode(node1, node2, node3)

	proxyServer := NewProxyServer(node1, node2, node3)
	client := NewClient(proxyServer)

	ctx := context.Background()
	client.Write(ctx, "key", "value")
}
