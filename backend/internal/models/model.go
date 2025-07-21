package models

import (
	"time"

	"github.com/google/uuid"
)

type PaymentRequest struct {
	CorrelationId uuid.UUID `json:"correlationId"`
	Amount        int64     `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"` // Add this field!
}

type Payment struct {
	PaymentRequest
	Service string `json:"-"` // Don't include in JSON
}

type PaymentSummary struct {
	Default  Summary `json:"default"`
	Fallback Summary `json:"fallback"`
}

type PaymentSummaryResponse struct {
	Default  SummaryResponse `json:"default"`
	Fallback SummaryResponse `json:"fallback"`
}

type SummaryResponse struct {
	TotalRequests int64   `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type Summary struct {
	TotalRequests int64 `json:"totalRequests"`
	TotalAmount   int64 `json:"totalAmount"`
}

type HealthCheckResponse struct {
	Failing         bool   `json:"failing"`
	MinResponseTime uint16 `json:"minResponseTime"`
}
