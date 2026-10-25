package observer

import (
	"context"

	"abd/client"
)

func NewObservedSetter(o Observer, s client.Setter) client.Setter {
	return &observedSetter{o, s}
}

type observedSetter struct {
	observer Observer
	setter   client.Setter
}

func (s *observedSetter) Set(ctx context.Context, value int) error {
	start := s.observer.Now()
	err := s.setter.Set(ctx, value)
	end := s.observer.Now()
	r := result{
		start:            start,
		end:              end,
		kind:             setOperation,
		seenWrittenValue: value,
	}
	s.observer.Add(r)
	return err
}
