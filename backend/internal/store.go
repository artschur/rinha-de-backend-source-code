package internal

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	redisClient *redis.Client
}

func (s *Store) StorePayment(ctx context.Context, payment Payment, service string, timestamp time.Time) error {
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

func (s *Store) getServiceSummary(ctx context.Context, service string, fromTime, toTime time.Time) summary {
	timeSetKey := fmt.Sprintf("payments:time:%s", service)

	paymentIDs, err := s.redisClient.ZRangeByScore(ctx, timeSetKey, &redis.ZRangeBy{
		Min: strconv.FormatInt(fromTime.Unix(), 10),
		Max: strconv.FormatInt(toTime.Unix(), 10),
	}).Result()

	if err != nil {
		if err == redis.Nil {
			log.Printf("No time set found for service %s - returning zero summary", service)
		} else {
			log.Printf("Error querying time set for %s: %v", service, err)
		}
		return summary{TotalRequests: 0, TotalAmount: 0}
	}

	if len(paymentIDs) == 0 {
		log.Printf("No payments found for service %s in time range", service)
		return summary{TotalRequests: 0, TotalAmount: 0}
	}

	totalRequests := int64(len(paymentIDs))
	totalAmount := 0.0

	// Use pipeline for efficient batch operations
	pipe := s.redisClient.Pipeline()
	for _, paymentID := range paymentIDs {
		paymentKey := fmt.Sprintf("payment:%s", paymentID)
		pipe.HGet(ctx, paymentKey, "amount")
	}

	results, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("Error getting payment amounts for %s: %v", service, err)
		return summary{TotalRequests: 0, TotalAmount: 0}
	}

	// Sum up all amounts
	for _, result := range results {
		if cmd, ok := result.(*redis.StringCmd); ok {
			if cmd.Err() == nil {
				if amount, err := strconv.ParseFloat(cmd.Val(), 64); err == nil {
					totalAmount += amount
				}
			}
		}
	}

	log.Printf("Service %s summary: %d requests, %.2f total amount", service, totalRequests, totalAmount)
	return summary{
		TotalRequests: totalRequests,
		TotalAmount:   totalAmount,
	}
}

func (s *Store) IncrementSummary(ctx context.Context, payment Payment, chosenService string) error {
	// Store individual payment for time-based queries
	err := s.StorePayment(ctx, payment, chosenService, payment.ReceivedAt)
	if err != nil {
		log.Printf("Warning: Failed to store individual payment: %v", err)
		// Don't return error - continue with summary increment
	}

	// Update aggregated summary counters
	var key string
	switch chosenService {
	case "default":
		key = "payment:summary:default"
	case "fallback":
		key = "payment:summary:fallback"
	default:
		return fmt.Errorf("unknown service: %s", chosenService)
	}

	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		err := s.redisClient.Watch(ctx, func(tx *redis.Tx) error {
			// Get current values
			currentReq := tx.HGet(ctx, key, "totalRequests").Val()
			currentAmt := tx.HGet(ctx, key, "totalAmount").Val()

			// Parse current values
			reqVal, _ := strconv.ParseInt(currentReq, 10, 64)
			amtVal, _ := strconv.ParseFloat(currentAmt, 64)

			// Start transaction
			pipe := tx.TxPipeline()
			pipe.HSet(ctx, key, "totalRequests", reqVal+1)
			pipe.HSet(ctx, key, "totalAmount", amtVal+payment.Amount)

			// Execute transaction
			_, err := pipe.Exec(ctx)
			return err
		}, key)

		if err == nil {
			log.Printf("✅ Successfully incremented %s: amount=%.2f", chosenService, payment.Amount)
			return nil
		}

		if err == redis.TxFailedErr {
			log.Printf("⚠️ Transaction conflict for %s, retry %d/%d", chosenService, i+1, maxRetries)
			time.Sleep(time.Duration(i+1) * time.Millisecond)
			continue
		}

		return fmt.Errorf("redis transaction failed: %w", err)
	}

	return fmt.Errorf("failed to increment %s after %d retries due to conflicts", chosenService, maxRetries)
}

func (s *Store) GetSummary(ctx context.Context) (*PaymentSummary, error) {
	summary := &PaymentSummary{}

	// Get default summary
	defaultResult, err := s.redisClient.HGetAll(ctx, "payment:summary:default").Result()
	if err != nil {
		log.Printf("ERROR: Failed to get default summary: %v", err)
		return nil, err
	}

	// Parse default summary
	if totalReq, ok := defaultResult["totalRequests"]; ok {
		if val, err := strconv.ParseInt(totalReq, 10, 64); err == nil {
			summary.Default.TotalRequests = val
		}
	}
	if totalAmt, ok := defaultResult["totalAmount"]; ok {
		if val, err := strconv.ParseFloat(totalAmt, 64); err == nil {
			summary.Default.TotalAmount = val
		}
	}

	// Get fallback summary
	fallbackResult, err := s.redisClient.HGetAll(ctx, "payment:summary:fallback").Result()
	if err != nil {
		log.Printf("ERROR: Failed to get fallback summary: %v", err)
		return nil, err
	}

	if totalReq, ok := fallbackResult["totalRequests"]; ok {
		if val, err := strconv.ParseInt(totalReq, 10, 64); err == nil {
			summary.Fallback.TotalRequests = val
		}
	}
	if totalAmt, ok := fallbackResult["totalAmount"]; ok {
		if val, err := strconv.ParseFloat(totalAmt, 64); err == nil {
			summary.Fallback.TotalAmount = val
		}
	}

	return summary, nil
}

func (s *Store) GetSummaryWithTime(ctx context.Context, from, to string) (*PaymentSummary, error) {
	var fromTime, toTime time.Time
	var err error

	if from != "" {
		fromTime, err = parseFlexibleTime(from)
		if err != nil {
			return nil, fmt.Errorf("invalid from date '%s': %w", from, err)
		}
	} else {
		fromTime = time.Unix(0, 0).UTC() // Beginning of time
	}

	if to != "" {
		toTime, err = parseFlexibleTime(to)
		if err != nil {
			return nil, fmt.Errorf("invalid to date '%s': %w", to, err)
		}
	} else {
		toTime = time.Now().UTC()
	}

	defaultSummary := s.getServiceSummary(ctx, "default", fromTime, toTime)
	fallbackSummary := s.getServiceSummary(ctx, "fallback", fromTime, toTime)

	return &PaymentSummary{
		Default:  defaultSummary,
		Fallback: fallbackSummary,
	}, nil
}

func (s *Store) PurgeAllData(ctx context.Context) error {
	// Get all payment-related keys
	paymentKeys, _ := s.redisClient.Keys(ctx, "payment:*").Result()
	timeSetKeys, _ := s.redisClient.Keys(ctx, "payments:time:*").Result()

	// Core summary keys
	keys := []string{
		"payment:summary:default",
		"payment:summary:fallback",
	}

	// Add all payment and time set keys
	keys = append(keys, paymentKeys...)
	keys = append(keys, timeSetKeys...)

	if len(keys) == 0 {
		log.Println("No keys to purge")
		return nil
	}

	// Delete all keys in batches
	batchSize := 100
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		pipe := s.redisClient.Pipeline()
		for _, key := range keys[i:end] {
			pipe.Del(ctx, key)
		}

		_, err := pipe.Exec(ctx)
		if err != nil {
			return fmt.Errorf("failed to purge batch: %w", err)
		}
	}

	log.Printf("Payment data purged successfully (%d keys deleted)", len(keys))
	return nil
}
