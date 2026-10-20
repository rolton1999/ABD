package main

import (
	"context"
	"sync"
	"sync/atomic"

	"abd/client"
)

type operationKind int

const (
	get operationKind = iota
	set
)

type result struct {
	start        uint64
	end          uint64
	kind         operationKind
	seenValue    int
	writtenValue int
}

type Observer interface {
	Now() uint64
	Add(r result)
	CheckLinearization() bool
}

type observerImpl struct {
	resultsMutex sync.Mutex
	results      []result

	monotonicClock atomic.Uint64
}

func NewObserver() Observer {
	return &observerImpl{}
}

func (c *observerImpl) Now() uint64 {
	return c.monotonicClock.Add(1)
}

func (c *observerImpl) Add(r result) {
	c.resultsMutex.Lock()
	defer c.resultsMutex.Unlock()

	c.results = append(c.results, r)
}

func (c *observerImpl) CheckLinearization() bool {
	c.resultsMutex.Lock()
	defer c.resultsMutex.Unlock()
	return true
}

type observedSetter struct {
	observer Observer
	setter   client.Setter
}

func NewObservedSetter(o Observer, s client.Setter) client.Setter {
	return &observedSetter{o, s}
}

func (s *observedSetter) Set(ctx context.Context, value int) error {
	start := s.observer.Now()
	err := s.setter.Set(ctx, value)
	end := s.observer.Now()
	r := result{
		start:        start,
		end:          end,
		kind:         set,
		writtenValue: value,
	}
	s.observer.Add(r)
	return err
}

type observedGetter struct {
	observer Observer
	getter   client.Getter
}

func NewObservedGetter(o Observer, g client.Getter) client.Getter {
	return &observedGetter{o, g}
}

func (s *observedGetter) Get(ctx context.Context) (int, error) {
	start := s.observer.Now()
	v, err := s.getter.Get(ctx)
	end := s.observer.Now()
	r := result{
		start:     start,
		end:       end,
		kind:      get,
		seenValue: v,
	}
	s.observer.Add(r)
	return v, err
}
