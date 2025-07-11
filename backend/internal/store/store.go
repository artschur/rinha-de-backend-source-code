package store

import (
	"context"
	"fmt"
	"rinha-backend-arthur/internal/models"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	redisClient *redis.Client
}

func (s *Store) StorePayment(ctx context.Context, payment models.Payment, service string) error {
	paymentKey := fmt.Sprintf("payment:%s", payment.CorrelationId)
	paymentData := map[string]interface{}{
		"correlationId": payment.CorrelationId.String(), // Convert UUID to string
		"amount":        payment.Amount,
		"service":       service,
		"timestamp":     timestamp.Unix(),
	}

	err := s.redisClient.HMSet(ctx, paymentKey, paymentData).Err()
	if err != nil {
		return fmt.Errorf("failed to store payment: %w", err)
	}

	// Add to sorted set for time-based queries
	timeSetKey := fmt.Sprintf("payments:time:%s", service)
	err = s.redisClient.ZAdd(ctx, timeSetKey, redis.Z{
		Score:  float64(timestamp.Unix()),
		Member: payment.CorrelationId.String(), // Convert UUID to string
	}).Err()

	if err != nil {
		return fmt.Errorf("failed to add to time set: %w", err)
	}

	return nil
}
