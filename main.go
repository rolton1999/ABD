// Attiya, Bar-Noy, and Dolev
package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"abd/client"
	"abd/cluster"
	observerP "abd/observer"
)

func main() {
	start := time.Now()

	for range 1000 {
		check(
			/*nodesAmount*/ 3,
			/*producersAmount*/ 2,
			/*consumersAmount*/ 2,
			/*shouldBeWritten*/ 10,
			/*checkLinearization*/ true,
		)
	}
	fmt.Println("checked in", time.Since(start))
}

func check(
	nodesAmount int,
	producersAmount int,
	consumersAmount int,
	shouldBeWritten int,
	checkLinearization bool,
) {
	nodes := cluster.NewCluster(nodesAmount)
	newRegister := func() cluster.Register {
		return cluster.NewRegister(nodes...)
	}

	producers := createClients(producersAmount, newRegister)

	ctx := context.Background()

	value := atomic.Int32{}

	observer := observerP.NewObserver()
	wgProducers := sync.WaitGroup{}
	startProduce := make(chan struct{})

	for i := range producers {
		wgProducers.Go(func() {
			setter := observerP.NewObservedSetter(observer, producers[i])
			<-startProduce
			for {
				next := value.Add(1)
				if next > int32(shouldBeWritten) {
					break
				}
				e := setter.Set(ctx, int(next))
				if e != nil {
					panic("cannot write")
				}
			}
		})
	}

	consumers := createClients(consumersAmount, newRegister)
	wgConsumers := sync.WaitGroup{}
	startConsume := make(chan struct{})
	stopConsume := atomic.Bool{}
	readValues := make([][]int, consumersAmount)
	for i := range consumers {
		wgConsumers.Go(func() {
			getter := observerP.NewObservedGetter(observer, consumers[i])
			<-startConsume
			for {
				v, e := getter.Get(ctx)
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

	for i := range readValues {
		readValues[i] = unify(readValues[i])
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

	if checkLinearization {
		start := time.Now()
		if !observer.CheckLinearization() {
			panic("not linearized")
		} else {
			fmt.Println("Linearization: linealized! checked in", time.Since(start))
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
