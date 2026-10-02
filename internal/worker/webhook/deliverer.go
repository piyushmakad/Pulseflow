package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"pulseflow/internal/domain"
	"pulseflow/internal/worker"
)

const maxResponseBodyBytes = 64 << 10

type Config struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
}

type Deliverer struct {
	client *http.Client
}

func New(client *http.Client) *Deliverer {
	if client == nil {
		client = &http.Client{}
	}
	return &Deliverer{client: client}
}

func (d *Deliverer) Deliver(ctx context.Context, delivery domain.DeliveryAttempt) worker.Outcome {
	var cfg Config
	if err := json.Unmarshal(delivery.DestinationConfig, &cfg); err != nil {
		return permanent(fmt.Sprintf("invalid webhook config: %v", err))
	}
	endpoint, err := url.Parse(cfg.URL)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return permanent("webhook config requires an absolute http or https URL")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(string(delivery.RequestPayload)))
	if err != nil {
		return permanent(fmt.Sprintf("create webhook request: %v", err))
	}
	for key, value := range cfg.Headers {
		req.Header.Set(key, value)
	}
	// PulseFlow owns these headers so a rule cannot accidentally replace the
	// stable identifiers downstream providers use for deduplication.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "PulseFlow/0.1")
	req.Header.Set("Idempotency-Key", delivery.ID)
	req.Header.Set("X-PulseFlow-Delivery-ID", delivery.ID)
	req.Header.Set("X-PulseFlow-Attempt", strconv.Itoa(delivery.AttemptNumber+1))

	resp, err := d.client.Do(req)
	if err != nil {
		return retryable(fmt.Sprintf("send webhook: %v", err), nil)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	bodyText := string(body)
	if readErr != nil {
		return retryable(fmt.Sprintf("read webhook response: %v", readErr), nil)
	}
	status := resp.StatusCode
	outcome := worker.Outcome{ResponseStatus: &status, ResponseBody: &bodyText}
	switch {
	case status >= 200 && status < 300:
		outcome.Kind = worker.OutcomeDelivered
	case status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500:
		outcome.Kind = worker.OutcomeRetryable
		message := fmt.Sprintf("webhook returned retryable HTTP status %d", status)
		outcome.ErrorMessage = &message
		outcome.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
	default:
		outcome.Kind = worker.OutcomePermanent
		message := fmt.Sprintf("webhook returned permanent HTTP status %d", status)
		outcome.ErrorMessage = &message
	}
	return outcome
}

func parseRetryAfter(value string, now time.Time) *time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		delay := time.Duration(seconds) * time.Second
		return &delay
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return nil
	}
	delay := when.Sub(now)
	return &delay
}

func permanent(message string) worker.Outcome {
	return worker.Outcome{Kind: worker.OutcomePermanent, ErrorMessage: &message}
}

func retryable(message string, retryAfter *time.Duration) worker.Outcome {
	return worker.Outcome{Kind: worker.OutcomeRetryable, ErrorMessage: &message, RetryAfter: retryAfter}
}
