package internal

import (
	"fmt"
	"log"
	"rinha-backend-arthur/internal/models"
	"time"
)

func buildSummary(payments []models.Payment) models.PaymentSummary {
	summary := models.PaymentSummary{
		Default:  models.Summary{},
		Fallback: models.Summary{},
	}

	for _, payments := range payments {
		switch payments.Service {
		case "default":
			summary.Default.TotalRequests++
			summary.Default.TotalAmount += payments.Amount
		case "fallback":
			summary.Fallback.TotalRequests++
			summary.Fallback.TotalAmount += payments.Amount
		default:
			// Ignore payments with unknown service
			log.Printf("Warning: Unknown service '%s' for payment with correlation ID %s", payments.Service, payments.CorrelationId)
			continue
		}
	}

	return summary
}

func PaymentsToSummary(payments []models.Payment, from, to time.Time) models.PaymentSummary {

	validPayments := []models.Payment{}
	switch {
	case from.IsZero() && to.IsZero():
		return buildSummary(payments)
	case !from.IsZero() && !to.IsZero():
		for _, payment := range payments {
			if payment.ReceivedAt.Before(from) || payment.ReceivedAt.After(to) {
				continue
			}
			validPayments = append(validPayments, payment)
		}
	}

	if len(validPayments) == 0 {
		return models.PaymentSummary{
			Default:  models.Summary{},
			Fallback: models.Summary{},
		}
	}
	return buildSummary(validPayments)
}

func ParseFlexibleTime(timeStr string) (time.Time, error) {
	if timeStr == "" {
		return time.Time{}, nil
	}

	// Formatos ISO 8601 UTC conforme especificação
	formats := []string{
		time.RFC3339,               // "2006-01-02T15:04:05Z07:00"
		"2006-01-02T15:04:05.000Z", // "2020-07-10T12:34:56.000Z"
		"2006-01-02T15:04:05Z",     // "2020-07-10T12:34:56Z"
		"2006-01-02T15:04:05",      // "2000-01-01T00:00:00" (without timezone)
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timeStr); err == nil {
			if t.Location() == time.UTC || format == "2006-01-02T15:04:05" || format == "2006-01-02" {
				return t.UTC(), nil
			}
			return t.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid ISO UTC date format '%s' (expected: 2020-07-10T12:34:56.000Z)", timeStr)
}
