package main

import "context"

type Client interface {
	Set(ctx context.Context, value int) error
	Get(ctx context.Context) (value int, err error)
}

func NewClient(proxyServer Register) Client {
	return &clientImpl{proxyServer}
}

type clientImpl struct {
	proxyServer Register
}

func (c *clientImpl) Set(ctx context.Context, value int) error {
	return c.proxyServer.Write(ctx, value)
}

func (c *clientImpl) Get(ctx context.Context) (int, error) {
	return c.proxyServer.Read(ctx)
}
