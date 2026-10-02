package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"pulseflow/internal/platform/logger"
)

type Runner interface {
	Run(context.Context) error
}

type Engine struct {
	components      []Runner
	pools           []*Pool
	shutdownTimeout time.Duration
	logger          *logger.Logger
}

func NewEngine(components []Runner, pools []*Pool, shutdownTimeout time.Duration, log *logger.Logger) (*Engine, error) {
	if len(components) == 0 || len(pools) == 0 || shutdownTimeout <= 0 || log == nil {
		return nil, fmt.Errorf("worker components, pools, shutdown timeout, and logger are required")
	}
	return &Engine{components: components, pools: pools, shutdownTimeout: shutdownTimeout, logger: log}, nil
}

func (e *Engine) Run(ctx context.Context) error {
	intakeCtx, stopIntake := context.WithCancel(ctx)
	defer stopIntake()
	errorsCh := make(chan error, len(e.components))
	var components sync.WaitGroup
	for _, component := range e.components {
		component := component
		components.Add(1)
		go func() {
			defer components.Done()
			errorsCh <- component.Run(intakeCtx)
		}()
	}

	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errorsCh:
		if err != nil {
			runErr = err
		} else {
			runErr = fmt.Errorf("worker component stopped unexpectedly")
		}
	}
	stopIntake()
	components.Wait()

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), e.shutdownTimeout)
	defer cancelDrain()
	for _, pool := range e.pools {
		if err := pool.CloseAndDrain(drainCtx); err != nil {
			e.logger.Warn("worker pool forced to stop after drain deadline", "error", err)
			runErr = errors.Join(runErr, err)
		}
	}
	return runErr
}
