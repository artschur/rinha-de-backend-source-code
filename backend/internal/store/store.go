package store

import (
	"context"
	"fmt"
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

	return nil
}

func (s *Store) GetAllPayments(ctx context.Context) ([]models.Payment, error) {
	keys, err := s.RedisClient.Keys(ctx, "payment:*").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve payment keys: %w", err)
	}

	if len(keys) == 0 {
		return []models.Payment{}, nil
	}

	var allPayments []models.Payment
	batchSize := 500 // Process in smaller batches

	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		batchPayments, err := s.getPaymentsBatch(ctx, keys[i:end])
		if err != nil {
			return nil, err
		}

		allPayments = append(allPayments, batchPayments...)
	}

	return allPayments, nil
}

func (s *Store) getPaymentsBatch(ctx context.Context, keys []string) ([]models.Payment, error) {
	pipe := s.RedisClient.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(keys))

	for i, key := range keys {
		cmds[i] = pipe.HGetAll(ctx, key)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	var payments []models.Payment
	for _, cmd := range cmds {
		data, err := cmd.Result()
		if err != nil || len(data) == 0 {
			continue
		}

		payment, err := s.parsePaymentFromRedisData(data)
		if err != nil {
			continue
		}

		payments = append(payments, payment)
	}

	return payments, nil
}

func (s *Store) parsePaymentFromRedisData(data map[string]string) (models.Payment, error) {
	correlationId, _ := data["correlationId"]
	amount, _ := data["amount"]
	service, _ := data["service"]
	timestamp, _ := data["timestamp"]

	amtFloat, err := strconv.ParseFloat(amount, 64)
	if err != nil {
		return models.Payment{}, fmt.Errorf("failed to parse amount: %w", err)
	}

	timestampParsed, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return models.Payment{}, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	payment := models.Payment{
		PaymentRequest: models.PaymentRequest{
			CorrelationId: uuid.MustParse(correlationId),
			Amount:        amtFloat,
			RequestedAt:   time.Unix(timestampParsed, 0).UTC(),
		},
		Service: service,
	}

	return payment, nil
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
