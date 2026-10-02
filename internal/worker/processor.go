package worker

import (
	"context"
	"fmt"
	"time"

	"pulseflow/internal/domain"
	"pulseflow/internal/platform/logger"
)

type Deliverer interface {
	Deliver(context.Context, domain.DeliveryAttempt) Outcome
}

type ResultStore interface {
	RecordDeliveryResult(context.Context, string, string, domain.DeliveryResult, string) (domain.DeliveryAttempt, error)
	FinalizeEvent(context.Context, string) (domain.EventStatus, bool, error)
}

type Processor struct {
	store              ResultStore
	deliverer          Deliverer
	policy             RetryPolicy
	owner              string
	deadLetterTopic    string
	persistenceTimeout time.Duration
	logger             *logger.Logger
}

func NewProcessor(store ResultStore, deliverer Deliverer, policy RetryPolicy, owner, deadLetterTopic string, persistenceTimeout time.Duration, log *logger.Logger) (*Processor, error) {
	if store == nil || deliverer == nil || owner == "" || deadLetterTopic == "" || persistenceTimeout <= 0 || log == nil {
		return nil, fmt.Errorf("result store, deliverer, owner, dead-letter topic, persistence timeout, and logger are required")
	}
	return &Processor{store: store, deliverer: deliverer, policy: policy, owner: owner,
		deadLetterTopic: deadLetterTopic, persistenceTimeout: persistenceTimeout, logger: log}, nil
}

func (p *Processor) Handle(ctx context.Context, delivery domain.DeliveryAttempt) {
	started := time.Now()
	outcome := p.deliverer.Deliver(ctx, delivery)
	result := p.policy.Result(delivery, outcome, time.Now(), time.Since(started))

	// A timed-out HTTP context must not prevent us recording that timeout. This
	// fresh, bounded context is used only for the PostgreSQL bookkeeping step.
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.persistenceTimeout)
	defer cancel()
	updated, err := p.store.RecordDeliveryResult(persistCtx, delivery.ID, p.owner, result, p.deadLetterTopic)
	if err != nil {
		p.logger.Error("failed to persist delivery result", "delivery_id", delivery.ID, "error", err)
		return
	}
	p.logger.Info("delivery attempt finished", "delivery_id", delivery.ID, "channel", delivery.Channel,
		"attempt", updated.AttemptNumber, "status", updated.Status)

	if !updated.Status.IsTerminal() {
		return
	}
	if _, _, err := p.store.FinalizeEvent(persistCtx, delivery.EventID); err != nil {
		// The periodic finalizer repairs this if PostgreSQL fails between TX4 and TX5.
		p.logger.Error("failed to finalize event", "event_id", delivery.EventID, "error", err)
	}
}
