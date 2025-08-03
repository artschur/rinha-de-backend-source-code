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
	// Use nanosecond precision for score
	score := float64(payment.RequestedAt.UnixNano())

	// Store in main payments sorted set
	err = s.RedisClient.ZAdd(ctx, "payments", redis.Z{
		Score:  score,
		Member: paymentJSON,
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to store payment: %w", err)
	}

	// Store in service-specific sorted set for efficient aggregation
	serviceKey := fmt.Sprintf("payments:%s", payment.Service)
	err = s.RedisClient.ZAdd(ctx, serviceKey, redis.Z{
		Score:  score,
		Member: strconv.FormatInt(payment.Amount, 10), // Store only amount for aggregation
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to store payment in service set: %w", err)
	}

	return nil
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

// GetPaymentSummaryByTime efficiently aggregates payment data using Redis operations
func (s *Store) GetPaymentSummaryByTime(ctx context.Context, from, to time.Time) (models.PaymentSummary, error) {
	minScore := float64(from.UnixNano())
	maxScore := float64(to.UnixNano())

	// Lua script for efficient server-side aggregation
	luaScript := `
		local function get_service_summary(service_key, min_score, max_score)
			local members = redis.call('ZRANGEBYSCORE', service_key, min_score, max_score)
			local total_amount = 0
			local total_requests = #members

			for i = 1, #members do
				local amount = tonumber(members[i])
				if amount then
					total_amount = total_amount + amount
				end
			end

			return {total_requests, total_amount}
		end

		local min_score = ARGV[1]
		local max_score = ARGV[2]

		local default_result = get_service_summary('payments:default', min_score, max_score)
		local fallback_result = get_service_summary('payments:fallback', min_score, max_score)

		return {
			default_result[1], default_result[2],
			fallback_result[1], fallback_result[2]
		}
	`

	result, err := s.RedisClient.Eval(ctx, luaScript, []string{},
		fmt.Sprintf("%f", minScore),
		fmt.Sprintf("%f", maxScore)).Result()
	if err != nil {
		return models.PaymentSummary{}, fmt.Errorf("failed to execute aggregation script: %w", err)
	}

	// Parse results
	results, ok := result.([]interface{})
	if !ok || len(results) != 4 {
		return models.PaymentSummary{}, fmt.Errorf("unexpected script result format")
	}

	defaultRequests, _ := results[0].(int64)
	defaultAmount, _ := results[1].(int64)
	fallbackRequests, _ := results[2].(int64)
	fallbackAmount, _ := results[3].(int64)

	return models.PaymentSummary{
		Default: models.Summary{
			TotalRequests: defaultRequests,
			TotalAmount:   defaultAmount,
		},
		Fallback: models.Summary{
			TotalRequests: fallbackRequests,
			TotalAmount:   fallbackAmount,
		},
	}, nil
}

// GetAllPaymentsSummary efficiently gets summary for all payments
func (s *Store) GetAllPaymentsSummary(ctx context.Context) (models.PaymentSummary, error) {
	// Use -inf and +inf for all time ranges
	return s.GetPaymentSummaryByTime(ctx, time.Unix(0, 0), time.Unix(1<<63-1, 0))
}

// GetPaymentSummaryByTimePipeline uses Redis pipeline for ultra-fast aggregation
func (s *Store) GetPaymentSummaryByTimePipeline(ctx context.Context, from, to time.Time) (models.PaymentSummary, error) {
	minScore := fmt.Sprintf("%f", float64(from.UnixNano()))
	maxScore := fmt.Sprintf("%f", float64(to.UnixNano()))

	// Use pipeline for parallel Redis operations
	pipe := s.RedisClient.Pipeline()

	// Count operations (no data transfer)
	defaultCountCmd := pipe.ZCount(ctx, "payments:default", minScore, maxScore)
	fallbackCountCmd := pipe.ZCount(ctx, "payments:fallback", minScore, maxScore)

	// Sum operations using Lua script for each service
	sumScript := `
		local members = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2])
		local total = 0
		for i = 1, #members do
			local amount = tonumber(members[i])
			if amount then
				total = total + amount
			end
		end
		return total
	`

	defaultSumCmd := pipe.Eval(ctx, sumScript, []string{"payments:default"}, minScore, maxScore)
	fallbackSumCmd := pipe.Eval(ctx, sumScript, []string{"payments:fallback"}, minScore, maxScore)

	// Execute all commands in one round trip
	_, err := pipe.Exec(ctx)
	if err != nil {
		return models.PaymentSummary{}, fmt.Errorf("failed to execute pipeline: %w", err)
	}

	// Get results
	defaultCount := defaultCountCmd.Val()
	fallbackCount := fallbackCountCmd.Val()
	defaultSum, _ := defaultSumCmd.Val().(int64)
	fallbackSum, _ := fallbackSumCmd.Val().(int64)

	return models.PaymentSummary{
		Default: models.Summary{
			TotalRequests: defaultCount,
			TotalAmount:   defaultSum,
		},
		Fallback: models.Summary{
			TotalRequests: fallbackCount,
			TotalAmount:   fallbackSum,
		},
	}, nil
}

func (s *Store) PurgeAllData(ctx context.Context) error {
	err := s.RedisClient.Del(ctx, "payments").Err()
	if err != nil {
		return fmt.Errorf("failed to delete payments: %w", err)
	}

	// Delete service-specific keys
	err = s.RedisClient.Del(ctx, "payments:default", "payments:fallback").Err()
	if err != nil {
		return fmt.Errorf("failed to delete service payments: %w", err)
	}

	keys, err := s.RedisClient.Keys(ctx, "payments:processing:*").Result()
	if err == nil && len(keys) > 0 {
		s.RedisClient.Del(ctx, keys...)
	}

	return nil
}
