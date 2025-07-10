package internal

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	redisClient *redis.Client
}

func (s *Store) IncrementSummary(ctx context.Context, amount float64, chosenService string) error {
	txn := s.redisClient.TxPipeline()

	switch chosenService {
	case "default":
		txn.HIncrBy(ctx, "payment:summary:default", "totalRequests", 1)
		txn.HIncrByFloat(ctx, "payment:summary:default", "totalAmount", amount)
	case "fallback":
		txn.HIncrBy(ctx, "payment:summary:fallback", "totalRequests", 1)
		txn.HIncrByFloat(ctx, "payment:summary:fallback", "totalAmount", amount)
	default:
		return fmt.Errorf("unknown service: %s", chosenService)
	}

	_, err := txn.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis transaction failed: %w", err)
	}

	return nil
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
