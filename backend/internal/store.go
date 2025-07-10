package internal

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	redisClient *redis.Client
}

func (s *Store) IncrementSummary(ctx context.Context, amount float64, chosenService string) error {
	pipe := s.redisClient.Pipeline()

	switch chosenService {
	case "default":
		pipe.HIncrByFloat(ctx, "payment:summary:default", "totalRequests", 1)
		pipe.HIncrByFloat(ctx, "payment:summary:default", "totalAmount", amount)
	case "fallback":
		pipe.HIncrByFloat(ctx, "payment:summary:fallback", "totalRequests", 1)
		pipe.HIncrByFloat(ctx, "payment:summary:fallback", "totalAmount", amount)
	default:
		return redis.Nil // or handle the error as needed
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) GetSummary(ctx context.Context) (*PaymentSummary, error) {
	var summary PaymentSummary
	defaultSummary := summary.defaultSummary
	fallbackSummary := summary.fallbackSummary

	err := s.redisClient.HGetAll(ctx, "payment:summary:default").Scan(&defaultSummary)
	if err != nil {
		return nil, err
	}

	err = s.redisClient.HGetAll(ctx, "payment:summary:fallback").Scan(&fallbackSummary)
	if err != nil {
		return nil, err
	}

	return &PaymentSummary{
		defaultSummary:  defaultSummary,
		fallbackSummary: fallbackSummary,
	}, nil
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
