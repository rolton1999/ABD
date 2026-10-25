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

	fmt.Println("History len: ", len(c.results))
	if len(c.results) == 0 {
		return true
	}
	if len(c.results) > maxCheckableLenOfHistory {
		fmt.Println("History is too big to check")
		return true
	}
	for i := range c.results {
		linearized := check(
			0,
			map[int]bool{},
			map[uint64]bool{},
			c.results,
			i,
			uint64(0),
		)
		if linearized {
			return true
		}
	}
	return false
}

const maxCheckableLenOfHistory = 32

func check(
	lastWrittenValue int,
	resultsAlreadyAdded map[int]bool,
	sequencesAlreadyChecked map[uint64]bool,
	results []result,
	i int,
	cur uint64,
) bool {
	if len(resultsAlreadyAdded) == len(results) {
		return true
	}

	// main linearization check
	for j, r := range results {
		if r.end <= results[i].start && !resultsAlreadyAdded[j] {
			delete(resultsAlreadyAdded, j)
			return false
		}
	}

	seqKey := cur | uint64(lastWrittenValue)<<maxCheckableLenOfHistory
	if sequencesAlreadyChecked[seqKey] {
		return false
	}
	sequencesAlreadyChecked[seqKey] = true

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

		if check(
			r.seenWrittenValue,
			resultsAlreadyAdded,
			sequencesAlreadyChecked,
			results,
			j,
			cur|(1<<j),
		) {
			return true
		}
		delete(resultsAlreadyAdded, j)
	}

	return false
}
