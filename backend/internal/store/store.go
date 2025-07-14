package store

import (
	"context"
	"fmt"
	"log"
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
	paymentKey := fmt.Sprintf("payment:%s", payment.CorrelationId)
	paymentData := map[string]any{
		"correlationId": payment.CorrelationId.String(), // Convert UUID to string
		"amount":        payment.Amount,
		"service":       payment.Service,
		"timestamp":     payment.RequestedAt.Unix(),
	}

	err := s.RedisClient.HMSet(ctx, paymentKey, paymentData).Err()
	if err != nil {
		return fmt.Errorf("failed to store payment: %w", err)
	}
	log.Println("saved payment")

	return nil
}

func (s *Store) GetAllPayments(ctx context.Context) ([]models.Payment, error) {
	keys, err := s.RedisClient.Keys(ctx, "payment:*").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve payment keys: %w", err)
	}

	var payments []models.Payment
	for _, key := range keys {
		data, err := s.RedisClient.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve payment data for key %s: %w", key, err)
		}

		correlationId, _ := data["correlationId"]
		amount, _ := data["amount"]
		service, _ := data["service"]
		timestamp, _ := data["timestamp"]
		amtFloat, err := strconv.ParseFloat(amount, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse amount for key %s: %w", key, err)
		}
		timestampParsed, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse timestamp for key %s: %w", key, err)
		}

		payment := models.Payment{
			PaymentRequest: models.PaymentRequest{
				CorrelationId: uuid.MustParse(correlationId), // Parse UUID from string
				Amount:        amtFloat,                      // Convert string to float64
				RequestedAt:   time.Unix(timestampParsed, 0).UTC(),
			},
			Service: service,
		}
		payments = append(payments, payment)
	}

	return payments, nil
}

func (s *Store) PurgeAllData(ctx context.Context) error {
	keys, err := s.RedisClient.Keys(ctx, "payment:*").Result()
	if err != nil {
		return fmt.Errorf("failed to retrieve payment keys: %w", err)
	}
	if len(keys) == 0 {
		return nil // No keys to delete
	}
	_, err = s.RedisClient.Del(ctx, keys...).Result()
	if err != nil {
		return fmt.Errorf("failed to delete payment keys: %w", err)
	}
	return nil
}
