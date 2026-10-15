package main

import (
	"context"
	"fmt"
	"sync"
)

func DoViaQuorum(
	ctx context.Context,
	toDos []func(ctx context.Context) bool,
	majority int,
) error {
	if len(toDos) < majority {
		panic(fmt.Sprintf("taking majority (%d) is impossible out of %d", majority, len(toDos)))
	}

	results := make(chan bool, len(toDos))
	defer close(results)

	wg := sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, doFunc := range toDos {
		wg.Go(func() {
			success := doFunc(ctx)
			select {
			case <-ctx.Done():
				// fmt.Println("gone by conext done")
				return
			case results <- success:
			}
			// fmt.Println("sucess written")
		})
	}

	failed := 0
	for majority > 0 && failed+majority != len(toDos) {
		// fmt.Println("maj, f: ", majority, failed)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r := <-results:
			if r {
				majority -= 1
			} else {
				failed += 1
			}
		}
	}
	if failed == len(toDos) || majority > 0 {
		return fmt.Errorf("failed on quorum")
	}

	return nil
}
