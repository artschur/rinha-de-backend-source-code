package models

import (
	"time"

	"github.com/google/uuid"
)

type Payment struct {
	CorrelationId uuid.UUID `json:"correlationId"`
	Amount        float64   `json:"amount"`
	ReceivedAt    time.Time `json:"receivedAt"`
}

type PaymentSummary struct {
	Default  summary `json:"default"`
	Fallback summary `json:"fallback"`
}

type summary struct {
	TotalRequests int64   `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}
