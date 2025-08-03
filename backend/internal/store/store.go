package store

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"rinha-backend-arthur/internal/models"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Store struct {
	RedisClient *redis.Client
}

func (s *Store) StorePayment(ctx context.Context, payment models.Payment) error {
	// Store the full payment data in sorted set for retrieval by time if needed
	paymentData := map[string]any{
		"correlationId":    payment.CorrelationId.String(),
		"amount":           payment.Amount,
		"paymentProcessor": payment.Service,
		"requestedAt":      payment.RequestedAt.Format(time.RFC3339Nano),
	}
	paymentJSON, err := json.Marshal(paymentData)
	if err != nil {
		return fmt.Errorf("failed to marshal payment data: %w", err)
	}

	// Use a pipeline for better performance
	pipe := s.RedisClient.Pipeline()

	// Store the full payment in sorted set
	score := float64(payment.RequestedAt.UnixNano())
	pipe.ZAdd(ctx, "payments", redis.Z{
		Score:  score,
		Member: paymentJSON,
	})

	// Update summary counters by service type
	statsKey := fmt.Sprintf("payments:stats:%s", payment.Service)
	pipe.HIncrBy(ctx, statsKey, "count", 1)
	pipe.HIncrBy(ctx, statsKey, "amount", payment.Amount)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to store payment: %w", err)
	}

	return nil
}

func (s *Store) GetPaymentSummaryDirect(ctx context.Context) (models.PaymentSummaryResponse, error) {
	// Get stats for both processors in a single pipeline
	pipe := s.RedisClient.Pipeline()
	defaultStats := pipe.HGetAll(ctx, "payments:stats:default")
	fallbackStats := pipe.HGetAll(ctx, "payments:stats:fallback")

	_, err := pipe.Exec(ctx)
	if err != nil {
		return models.PaymentSummaryResponse{}, fmt.Errorf("failed to retrieve payment stats: %w", err)
	}

	// Process default stats
	defaultResult, _ := defaultStats.Result()
	defaultCount, _ := strconv.ParseInt(defaultResult["count"], 10, 64)
	defaultAmount, _ := strconv.ParseInt(defaultResult["amount"], 10, 64)

	// Process fallback stats
	fallbackResult, _ := fallbackStats.Result()
	fallbackCount, _ := strconv.ParseInt(fallbackResult["count"], 10, 64)
	fallbackAmount, _ := strconv.ParseInt(fallbackResult["amount"], 10, 64)

	return models.PaymentSummaryResponse{
		Default: models.SummaryResponse{
			TotalRequests: defaultCount,
			TotalAmount:   float64(defaultAmount) / 100.0,
		},
		Fallback: models.SummaryResponse{
			TotalRequests: fallbackCount,
			TotalAmount:   float64(fallbackAmount) / 100.0,
		},
	}, nil
}

func (s *Store) GetPaymentsByTime(ctx context.Context, from, to time.Time) ([]models.Payment, error) {
	minScore := float64(from.UnixNano())
	maxScore := float64(to.UnixNano())
	results, err := s.RedisClient.ZRangeByScore(ctx, "payments", &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", minScore),
		Max: fmt.Sprintf("%f", maxScore),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve payments: %w", err)
	}
	var payments []models.Payment
	for _, paymentDataJSON := range results {
		var paymentData map[string]interface{}
		if err := json.Unmarshal([]byte(paymentDataJSON), &paymentData); err != nil {
			continue // Skip malformed data
		}
		payment, err := s.parsePaymentFromData(paymentData)
		if err != nil {
			continue // Skip invalid data
		}
		payments = append(payments, payment)
	}
	return payments, nil
}

func (s *Store) GetAllPayments(ctx context.Context) ([]models.Payment, error) {
	// Get all payments from the single hash
	paymentsData, err := s.RedisClient.HGetAll(ctx, "payments").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve payments: %w", err)
	}

	if len(paymentsData) == 0 {
		return []models.Payment{}, nil
	}

	var payments []models.Payment
	for _, paymentDataJSON := range paymentsData {
		var paymentData map[string]interface{}
		if err := json.Unmarshal([]byte(paymentDataJSON), &paymentData); err != nil {
			continue // Skip malformed data
		}

		payment, err := s.parsePaymentFromData(paymentData)
		if err != nil {
			continue // Skip invalid data
		}

		payments = append(payments, payment)
	}

	return payments, nil
}

func (s *Store) parsePaymentFromData(data map[string]interface{}) (models.Payment, error) {
	correlationIdStr, ok := data["correlationId"].(string)
	if !ok {
		return models.Payment{}, fmt.Errorf("invalid correlationId")
	}

	var amount int64
	switch amountVal := data["amount"].(type) {
	case float64:
		amount = int64(math.Round(amountVal)) // Convert existing float to int
	case json.Number:
		floatVal, _ := amountVal.Float64()
		amount = int64(math.Round(floatVal))
	case int64:
		amount = amountVal
	case int:
		amount = int64(amountVal)
	default:
		return models.Payment{}, fmt.Errorf("invalid amount type: %T", data["amount"])
	}

	service, ok := data["paymentProcessor"].(string)
	if !ok {
		return models.Payment{}, fmt.Errorf("invalid paymentProcessor")
	}

	requestedAtStr, ok := data["requestedAt"].(string)
	if !ok {
		return models.Payment{}, fmt.Errorf("invalid requestedAt")
	}

	correlationId, err := uuid.Parse(correlationIdStr)
	if err != nil {
		return models.Payment{}, fmt.Errorf("failed to parse correlationId: %w", err)
	}

	requestedAt, err := time.Parse(time.RFC3339Nano, requestedAtStr)
	if err != nil {
		return models.Payment{}, fmt.Errorf("failed to parse requestedAt: %w", err)
	}

	payment := models.Payment{
		PaymentRequest: models.PaymentRequest{
			CorrelationId: correlationId,
			Amount:        amount,
			RequestedAt:   requestedAt.UTC(),
		},
		Service: service,
	}

	return payment, nil
}

func (s *Store) PurgeAllData(ctx context.Context) error {
	pipe := s.RedisClient.Pipeline()

	// Delete payments data
	pipe.Del(ctx, "payments")

	// Delete stats
	pipe.Del(ctx, "payments:stats:default")
	pipe.Del(ctx, "payments:stats:fallback")

	// Delete processing keys if they exist
	keys, _ := s.RedisClient.Keys(ctx, "payments:processing:*").Result()
	if len(keys) > 0 {
		pipe.Del(ctx, keys...)
	}

	_, err := pipe.Exec(ctx)
	return err
}
