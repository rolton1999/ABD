package cluster

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

	results := make(chan bool)
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
				return
			case results <- success:
			}
		})
	}

	var fail, success int
	for success < majority && fail+success != len(toDos) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r := <-results:
			if r {
				success += 1
			} else {
				fail += 1
			}
		}
	}
	if success < majority {
		return fmt.Errorf("failed on quorum")
	}

	return nil
}
