package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ----------------------------------------------------------------------------

type Client interface {
	Write(ctx context.Context, key string, value string) error
	Read(ctx context.Context, key string) (value string, err error)
}

func NewClient(ctx context.Context, proxyServer Server) Client {
	return &clientImpl{proxyServer}
}

type clientImpl struct {
	proxyServer Server
}

func (c *clientImpl) Write(ctx context.Context, key string, value string) error {
	return nil
}

func (c *clientImpl) Read(ctx context.Context, key string) (string, error) {
	return "", nil
}

// ----------------------------------------------------------------------------

type Server interface {
	ProcessWrite(ctx context.Context, key string, value string) error
	ProcessRead(ctx context.Context, key string) (value string, err error)
}

func NewProxyServer(nodes []Coordinator) Server {
	return &proxyServer{nodes}
}

// choose node as coordinator
type proxyServer struct {
	nodes []Coordinator
}

func (s *proxyServer) ProcessWrite(ctx context.Context, key, value string) error {
	return nil
}

func (s *proxyServer) ProcessRead(ctx context.Context, key string) (string, error) {
	return "", nil
}

// ----------------------------------------------------------------------------

type Coordinator interface {
	Client
	// Server
	// Write(key string, value string) error
	// Read(key string) (string, error)
}

type Replica interface {
	Server
	// Write(key string, value string) error
	// Read(key string) (string, error)
}

type Node interface {
	Coordinator
	Replica
}

func NewNode() Node {
	return &nodeImpl{}
}

type StampedValue struct {
	value     string
	timestamp uint64
}

type nodeImpl struct {
	stampedValues []StampedValue
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
	fmt.Println("vim-go")
}
