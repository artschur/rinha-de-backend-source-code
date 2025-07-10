package internal

import (
	"github.com/google/uuid"
)

type Payment struct {
	CorrelationId uuid.UUID `json:"correlationId"`
	Amount        float64   `json:"amount"`
	ReceivedAt    string    `json:"receivedAt"`
}

type PaymentSummary struct {
	Default  defaultSummary  `json:"default"`
	Fallback fallbackSummary `json:"fallback"`
}

type defaultSummary struct {
	TotalRequests int64   `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type fallbackSummary struct {
	TotalRequests int64   `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}
