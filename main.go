// Attiya, Bar-Noy, and Dolev
package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"abd/client"
	"abd/cluster"
)

func main() {
	const (
		nodesAmount     = 3
		producersAmount = 30
		consumersAmount = 30

		shouldBeWritten = 1_000_000
	)

	nodes := cluster.NewCluster(nodesAmount)
	newRegister := func() cluster.Register {
		return cluster.NewRegister(nodes...)
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

	// fmt.Println("from nodes:")
	// for i := range nodes {
	// 	fmt.Println(nodes[i].dumpSeenValues())
	// }

	fmt.Println("readers seen:")
	for i := range readValues {
		readValues[i] = unify(readValues[i])
		fmt.Println(len(readValues[i]))
	}

	replicated := map[int]map[int]bool{}
	for i, node := range nodes {
		h := map[int]bool{}
		for _, v := range node.DumpActions() {
			if h[v] {
				// helping phase can rewrite the save value on the same node or write retries
				continue
			}
			h[v] = true
			if replicated[v] == nil {
				replicated[v] = map[int]bool{}
			}
			replicated[v][i] = true
		}
	}

	notReplicated := map[int]bool{}
	for k, v := range replicated {
		if len(v) < 2 {
			notReplicated[k] = true
		}
	}

	// every reader can read only replicated values
	for i := range readValues {
		for _, v := range readValues[i] {
			if notReplicated[v] {
				panic("reader have seen not replicated value")
			}
		}
	}
	// check if of all read for every reader were monotonic
	for _, c := range consumers {
		sv := c.DumpSeenValues()
		for j := 1; j < len(sv); j++ {
			if sv[j-1].Timestamp.Greater(sv[j].Timestamp) {
				panic("reader have seen not monotonic writes")
			}
		}
	}
}

func createClients(n int, newRegister func() cluster.Register) []client.Client {
	var clients []client.Client
	for range n {
		clients = append(clients, client.NewClient(newRegister()))
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
