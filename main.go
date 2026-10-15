// Attiya, Bar-Noy, and Dolev
package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// ----------------------------------------------------------------------------

func main() {
	const (
		nodesAmount     = 3
		producersAmount = 3
		consumersAmount = 3

		shouldBeWritten = 100
	)

	nodes := NewNodes(nodesAmount)
	newRegister := func() Register {
		return NewRegister(nodes...)
	}

	producers := createClients(producersAmount, newRegister)

	ctx := context.Background()

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
				e := p.Set(ctx, int(next))
				if e != nil {
					panic("cannot write")
				}
			}
			fmt.Println(i, "writing done!")
		})
	}

	consumers := createClients(consumersAmount, newRegister)
	wgConsumers := sync.WaitGroup{}
	startConsume := make(chan struct{})
	stopConsume := atomic.Bool{}
	readValues := [consumersAmount][]int{}
	for i, c := range consumers {
		wgConsumers.Go(func() {
			<-startConsume
			for {
				v, e := c.Get(ctx)
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

func createClients(n int, newRegister func() Register) []Client {
	var clients []Client
	for range n {
		clients = append(clients, NewClient(newRegister()))
	}
	return clients
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
