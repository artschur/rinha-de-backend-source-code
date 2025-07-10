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

func (s *Store) IncrementSummary(ctx context.Context, amount float64, chosenService string) error {
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
	for i := range 5 {
		// Watch the key for changes
		err := s.redisClient.Watch(ctx, func(tx *redis.Tx) error {
			// Get current values (this establishes the watch)
			currentReq := tx.HGet(ctx, key, "totalRequests").Val()
			currentAmt := tx.HGet(ctx, key, "totalAmount").Val()

			// Parse current values
			reqVal, _ := strconv.ParseInt(currentReq, 10, 64)
			amtVal, _ := strconv.ParseFloat(currentAmt, 64)

			// Start transaction
			pipe := tx.TxPipeline()

			// Set new values atomically
			pipe.HSet(ctx, key, "totalRequests", reqVal+1)
			pipe.HSet(ctx, key, "totalAmount", amtVal+amount)

			// Execute transaction
			_, err := pipe.Exec(ctx)
			return err
		}, key)

		if err == nil {
			log.Printf("✅ Successfully incremented %s: amount=%.2f", chosenService, amount)
			return nil
		}

		if err == redis.TxFailedErr {
			// Transaction failed due to watched key being modified, retry
			log.Printf("⚠️ Transaction conflict for %s, retry %d", chosenService, i+1)
			time.Sleep(time.Duration(i+1) * time.Millisecond)
			continue
		}

		// Other error, return
		return fmt.Errorf("redis transaction failed: %w", err)
	}

	return fmt.Errorf("failed to increment %s after %d retries due to conflicts", chosenService, maxRetries)
}

func (s *Store) GetSummary(ctx context.Context) (*PaymentSummary, error) {
	log.Printf("GetSummary called")

	summary := &PaymentSummary{}

	// Get default summary
	defaultResult, err := s.redisClient.HGetAll(ctx, "payment:summary:default").Result()
	if err != nil {
		log.Printf("ERROR: Failed to get default summary: %v", err)
		return nil, err
	}
	log.Printf("Default Redis data: %+v", defaultResult)

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
	log.Printf("Fallback Redis data: %+v", fallbackResult)

	// Parse fallback summary
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

	log.Printf("Final summary: %+v", summary)
	return summary, nil
}

func (s *Store) PurgeAllData(ctx context.Context) error {
	// Delete all payment summary keys
	keys := []string{
		"payment:summary:default",
		"payment:summary:fallback",
	}

	pipe := s.redisClient.Pipeline()

	for _, key := range keys {
		pipe.Del(ctx, key)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to purge payment data: %w", err)
	}

	log.Println("Payment data purged successfully")
	return nil
}
