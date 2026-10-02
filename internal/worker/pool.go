package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"pulseflow/internal/domain"
)

type TaskHandler func(context.Context, domain.DeliveryAttempt)

// Pool has a fixed number of goroutines and a bounded channel. The channel is
// the waiting room: once it is full, dispatchers stop claiming PostgreSQL rows.
type Pool struct {
	jobs    chan domain.DeliveryAttempt
	handler TaskHandler
	timeout time.Duration

	workCtx context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	mu       sync.RWMutex
	closed   bool
	closeOne sync.Once
}

func NewPool(workerCount, bufferSize int, timeout time.Duration, handler TaskHandler) (*Pool, error) {
	if workerCount < 1 || bufferSize < 1 || timeout <= 0 || handler == nil {
		return nil, fmt.Errorf("positive worker count, buffer size, timeout, and handler are required")
	}
	workCtx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		jobs: make(chan domain.DeliveryAttempt, bufferSize), handler: handler,
		timeout: timeout, workCtx: workCtx, cancel: cancel,
	}
	for i := 0; i < workerCount; i++ {
		p.wg.Add(1)
		go p.runWorker()
	}
	return p, nil
}

func (p *Pool) runWorker() {
	defer p.wg.Done()
	for delivery := range p.jobs {
		ctx, cancel := context.WithTimeout(p.workCtx, p.timeout)
		p.handler(ctx, delivery)
		cancel()
	}
}

func (p *Pool) Available() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return 0
	}
	return cap(p.jobs) - len(p.jobs)
}

func (p *Pool) Submit(ctx context.Context, delivery domain.DeliveryAttempt) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return fmt.Errorf("worker pool is closed")
	}
	select {
	case p.jobs <- delivery:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CloseAndDrain stops accepting work but lets queued and active jobs finish.
// If the deadline expires, it cancels their contexts and waits for exit.
func (p *Pool) CloseAndDrain(ctx context.Context) error {
	p.closeOne.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.jobs)
		p.mu.Unlock()
	})

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.cancel()
		<-done
		return ctx.Err()
	}
}
