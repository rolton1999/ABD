package cluster

import (
	"context"
	"sync/atomic"
)

type Register interface {
	Write(ctx context.Context, value int) error
	Read(ctx context.Context) (value StampedValue, err error)
}

func NewRegister(coordinators ...Coordinator) Register {
	if len(coordinators) == 0 {
		panic("trying to setup proxy without nodex")
	}
	return &proxyRegister{atomic.Uint64{}, coordinators}
}

type proxyRegister struct {
	index atomic.Uint64
	nodes []Coordinator
}

func (s *proxyRegister) Write(ctx context.Context, value int) error {
	nodeIndex := s.getNodeIndex()
	return s.nodes[nodeIndex].ABDWrite(ctx, value)
}

func (s *proxyRegister) Read(ctx context.Context) (StampedValue, error) {
	nodeIndex := s.getNodeIndex()
	return s.nodes[nodeIndex].ABDRead(ctx)
}

func (s *proxyRegister) getNodeIndex() int {
	return int(s.index.Add(1) % uint64(len(s.nodes)))
}
