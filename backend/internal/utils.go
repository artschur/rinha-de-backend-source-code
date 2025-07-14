package internal

import (
	"fmt"
	"rinha-backend-arthur/internal/models"
	"time"
)

func buildSummary(payments []models.Payment) models.PaymentSummary {
	summary := models.PaymentSummary{
		Default:  models.Summary{},
		Fallback: models.Summary{},
	}

	for _, payment := range payments {
		switch payment.Service {
		case "default":
			summary.Default.TotalRequests++
			summary.Default.TotalAmount += payment.Amount
		case "fallback":
			summary.Fallback.TotalRequests++
			summary.Fallback.TotalAmount += payment.Amount
		}
	}

	// Round to 2 decimal places to match payment processor behavior
	// summary.Default.TotalAmount = math.Round(summary.Default.TotalAmount*100) / 100
	// summary.Fallback.TotalAmount = math.Round(summary.Fallback.TotalAmount*100) / 100

	return summary
}

func parseTimeRange(fromStr, toStr string) (from, to time.Time, err error) {
	// Both are optional - can have none, one, or both
	if fromStr == "" && toStr == "" {
		return // Both zero values means no filtering
	}

	// Parse from if provided
	if fromStr != "" {
		from, err = ParseFlexibleTime(fromStr) // Use your capitalized function
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid 'from' time format: %w", err)
		}
	}

	// Parse to if provided
	if toStr != "" {
		to, err = ParseFlexibleTime(toStr) // Use your capitalized function
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid 'to' time format: %w", err)
		}
	}

	// Validate that from is before to (if both are provided)
	if !from.IsZero() && !to.IsZero() && from.After(to) {
		return time.Time{}, time.Time{}, fmt.Errorf("'from' must be before or equal to 'to'")
	}

	return from, to, nil
}

func PaymentsToSummary(payments []models.Payment, from, to time.Time) models.PaymentSummary {
	validPayments := []models.Payment{}

	for _, payment := range payments {
		include := true

		if !from.IsZero() && payment.RequestedAt.Before(from) {
			include = false
		}
		if !to.IsZero() && payment.RequestedAt.After(to) {
			include = false
		}

		if include {
			validPayments = append(validPayments, payment)
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
