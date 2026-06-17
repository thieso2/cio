// Package gclient holds the lazy, process-wide initialization shared by every
// GCP client wrapper in this repo (storage, bigquery, compute, cloudrun, ...).
//
// Each service package used to hand-roll the same sync.Once + cached-(value,error)
// + Close() shape. Provider collapses that into one deep module: the service
// package supplies only the constructor closure and (optionally) how to close the
// value; Provider owns the once-semantics, error caching, and "was it created?"
// bookkeeping that Close needs.
package gclient

import (
	"context"
	"sync"
)

// Provider lazily creates a single value of type T and caches it (and any error)
// for the life of the process. T is expected to be a client/service pointer.
type Provider[T any] struct {
	once sync.Once
	val  T
	err  error
	ok   bool // true once init ran and returned no error
}

// Get returns the cached value, creating it via init on first call only.
// init runs at most once; later calls return the cached (value, error) and the
// init passed to them is ignored. This preserves the previous behaviour where
// arguments captured by the first caller (e.g. a projectID) win.
func (p *Provider[T]) Get(ctx context.Context, init func(context.Context) (T, error)) (T, error) {
	p.once.Do(func() {
		p.val, p.err = init(ctx)
		p.ok = p.err == nil
	})
	return p.val, p.err
}

// Close invokes closeFn with the cached value, but only if initialization ran
// and succeeded. It is a no-op when the client was never created, mirroring the
// old `if client != nil { client.Close() }` guard.
func (p *Provider[T]) Close(closeFn func(T) error) error {
	if p.ok {
		return closeFn(p.val)
	}
	return nil
}
