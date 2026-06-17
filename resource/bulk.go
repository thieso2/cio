package resource

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// confirm asks the user whether to proceed unless force is set. It prints
// prompt, reads a y/N answer, and prints "Aborted." when declined. It returns
// whether the caller should proceed.
//
// This is the single seam for destructive-operation confirmation in the
// resource layer — callers print their own preview of what will change, then
// gate the mutation on confirm().
func confirm(force bool, prompt string) bool {
	if force {
		return true
	}
	fmt.Print(prompt)
	var response string
	fmt.Scanln(&response)
	if response == "y" || response == "Y" {
		return true
	}
	fmt.Println("Aborted.")
	return false
}

// bulkRun runs action over every item in parallel, printing one line per item
// — "<verb>: <name> (took <elapsed>)" on success or "Failed: <name> (<err>)"
// on failure — and returns the first error encountered.
//
// name maps an item to its display name; the action closure captures whatever
// else it needs (project, region, the service-delete func). This is the one
// place the goroutine + mutex + WaitGroup + per-item timing + first-error
// bookkeeping lives, replacing a block that was copy-pasted across the Cloud
// SQL, Cloud Run, and Pub/Sub resources.
func bulkRun[T any](ctx context.Context, items []T, name func(T) string, verb string, action func(context.Context, T) error) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error

	for _, item := range items {
		wg.Add(1)
		go func(it T) {
			defer wg.Done()
			start := time.Now()
			err := action(ctx, it)
			elapsed := time.Since(start).Round(time.Millisecond)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				fmt.Printf("Failed: %s (%v)\n", name(it), err)
				if firstErr == nil {
					firstErr = err
				}
			} else {
				fmt.Printf("%s: %s (took %s)\n", verb, name(it), elapsed)
			}
		}(item)
	}
	wg.Wait()
	return firstErr
}
