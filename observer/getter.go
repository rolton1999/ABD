package observer

import (
	"context"

	"abd/client"
)

func NewObservedGetter(o Observer, g client.Getter) client.Getter {
	return &observedGetter{o, g}
}

type observedGetter struct {
	observer Observer
	getter   client.Getter
}

func (s *observedGetter) Get(ctx context.Context) (int, error) {
	start := s.observer.Now()
	v, err := s.getter.Get(ctx)
	end := s.observer.Now()
	r := result{
		start:            start,
		end:              end,
		kind:             getOperation,
		seenWrittenValue: v,
	}
	s.observer.Add(r)
	return v, err
}
