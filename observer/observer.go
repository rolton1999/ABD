package observer

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type operationKind int

const (
	getOperation operationKind = iota
	setOperation
)

type result struct {
	start            uint64
	end              uint64
	kind             operationKind
	seenWrittenValue int
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

	// fmt.Println("concurrent history:", len(c.results), c.results)
	// sort.Slice(c.results, func(i, j int) bool {
	// 	return c.results[i].start < c.results[j].start
	// })
	fmt.Println("History len: ", len(c.results))
	if len(c.results) > maxCheckableLenOfHistory {
		fmt.Println("History is too big to check")
		return true
	}
	current = nil
	return check(
		0,
		map[int]bool{},
		map[uint64]bool{},
		c.results,
		0,
		uint64(0),
	)
}

var current []result

const maxCheckableLenOfHistory = 25

func check(
	lastWrittenValue int,
	resultsAlreadyAdded map[int]bool,
	sequencesAlreadyChecked map[uint64]bool,
	results []result,
	i int,
	cur uint64,
) bool {
	if len(resultsAlreadyAdded) == len(results) {
		// fmt.Println(current)
		return true
	}

	for j, r := range results {
		if r.kind == getOperation && r.seenWrittenValue != lastWrittenValue {
			continue
		}
		if r.start > r.end {
			continue
		}

		if resultsAlreadyAdded[j] {
			continue
		}
		resultsAlreadyAdded[j] = true

		seq := cur | (1 << j) | uint64(lastWrittenValue)<<maxCheckableLenOfHistory
		if sequencesAlreadyChecked[seq] {
			resultsAlreadyAdded[j] = false
			continue
		}
		sequencesAlreadyChecked[seq] = true

		current = append(current, r)
		if check(
			r.seenWrittenValue,
			resultsAlreadyAdded,
			sequencesAlreadyChecked,
			results,
			i,
			cur|(1<<j),
		) {
			return true
		}
		current = current[:len(current)-1]
		delete(resultsAlreadyAdded, j)
	}

	return false
}
