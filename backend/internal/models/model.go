package models

import (
	"time"

	"github.com/google/uuid"
)

type PaymentRequest struct {
	Amount        int64     `json:"amount"`
	CorrelationId uuid.UUID `json:"correlationId"`
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
	TotalAmount   float64 `json:"totalAmount"`
	TotalRequests int64   `json:"totalRequests"`
}

type Summary struct {
	TotalRequests int64 `json:"totalRequests"`
	TotalAmount   int64 `json:"totalAmount"`
}

type HealthCheckResponse struct {
	MinResponseTime uint16 `json:"minResponseTime"`
	Failing         bool   `json:"failing"`
}
