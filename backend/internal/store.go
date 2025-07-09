package internal

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	redisClient *redis.Client
}

func (s *Store) IncrementSummary(ctx context.Context, amount float64) error {
	pipe := s.redisClient.Pipeline()

	pipe.HIncrByFloat(ctx, "payment:summary:default", "totalRequests", 1)
	pipe.HIncrByFloat(ctx, "payment:summary:default", "totalAmount", amount)

	pipe.HIncrByFloat(ctx, "payment:summary:fallback", "totalRequests", 1)
	pipe.HIncrByFloat(ctx, "payment:summary:fallback", "totalAmount", amount)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) GetSummary(ctx context.Context) (summary *PaymentSummary, err error) {
	defaultSummary := summary.defaultSummary
	fallbackSummary := summary.fallbackSummary

	err = s.redisClient.HGetAll(ctx, "payment:summary:default").Scan(&defaultSummary)
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
