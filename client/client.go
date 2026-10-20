package client

import (
	"context"

	"abd/cluster"
)

type Setter interface {
	Set(ctx context.Context, value int) error
}

type Getter interface {
	Get(ctx context.Context) (value int, err error)
}

type Client interface {
	Setter
	Getter

	DumpSeenValues() []cluster.StampedValue
}

func NewClient(proxyServer cluster.Register) Client {
	return &clientImpl{proxyServer, nil}
}

type clientImpl struct {
	proxyServer cluster.Register

	readValues []cluster.StampedValue
}

func (c *clientImpl) Set(ctx context.Context, value int) error {
	return c.proxyServer.Write(ctx, value)
}

func (c *clientImpl) Get(ctx context.Context) (int, error) {
	sv, err := c.proxyServer.Read(ctx)
	if err == nil {
		c.readValues = append(c.readValues, sv)
	}
	return sv.Value, err
}

func (c *clientImpl) DumpSeenValues() []cluster.StampedValue {
	return c.readValues
}
