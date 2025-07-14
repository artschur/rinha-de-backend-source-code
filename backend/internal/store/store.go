package store

import (
	"context"
	"encoding/json"
	"fmt"
	"rinha-backend-arthur/internal/models"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Store struct {
	RedisClient *redis.Client
}

func (s *Store) StorePayment(ctx context.Context, payment models.Payment) error {
	// Use a single hash key for all payments like the reference implementation
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

	err = s.RedisClient.HSet(ctx, "payments", payment.CorrelationId.String(), paymentJSON).Err()
	if err != nil {
		return fmt.Errorf("failed to store payment: %w", err)
	}

	return nil
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

	amount, ok := data["amount"].(float64)
	if !ok {
		return models.Payment{}, fmt.Errorf("invalid amount")
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
	err := s.RedisClient.Del(ctx, "payments").Err()
	if err != nil {
		return fmt.Errorf("failed to delete payments: %w", err)
	}

	keys, err := s.RedisClient.Keys(ctx, "payments:processing:*").Result()
	if err == nil && len(keys) > 0 {
		s.RedisClient.Del(ctx, keys...)
	}

	return nil
}
