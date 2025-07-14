package health

import "github.com/redis/go-redis/v9"

type Store struct {
	redis *redis.Client
}

func NewStore(redisClient *redis.Client) *Store {
	return &Store{
		redis: redisClient,
	}
}
