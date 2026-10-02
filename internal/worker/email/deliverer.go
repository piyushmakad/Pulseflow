package email

import (
	"context"
	"encoding/json"
	"fmt"
	"net/mail"
	"strings"

	"pulseflow/internal/domain"
	"pulseflow/internal/platform/logger"
	"pulseflow/internal/worker"
)

type Config struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

// Deliverer intentionally simulates email in Phase 5. A real provider would
// add an SDK/HTTP dependency and credentials before the delivery machinery is
// proven; the durable state and pool behavior are identical either way.
type Deliverer struct {
	logger *logger.Logger
}

func New(log *logger.Logger) *Deliverer {
	return &Deliverer{logger: log}
}

func (d *Deliverer) Deliver(ctx context.Context, delivery domain.DeliveryAttempt) worker.Outcome {
	if err := ctx.Err(); err != nil {
		message := fmt.Sprintf("email delivery cancelled: %v", err)
		return worker.Outcome{Kind: worker.OutcomeRetryable, ErrorMessage: &message}
	}
	var cfg Config
	if err := json.Unmarshal(delivery.DestinationConfig, &cfg); err != nil {
		return permanent(fmt.Sprintf("invalid email config: %v", err))
	}
	if _, err := mail.ParseAddress(cfg.To); err != nil || strings.TrimSpace(cfg.Subject) == "" {
		return permanent("email config requires a valid to address and non-empty subject")
	}
	d.logger.Info("simulated email delivered", "delivery_id", delivery.ID, "to", cfg.To, "subject", cfg.Subject)
	return worker.Outcome{Kind: worker.OutcomeDelivered}
}

func permanent(message string) worker.Outcome {
	return worker.Outcome{Kind: worker.OutcomePermanent, ErrorMessage: &message}
}
