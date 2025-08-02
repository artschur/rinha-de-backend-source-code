package internal

import (
	"os"
	"strings"
)

type Config struct {
	RedisURL string
	Workers  int
	Port     int
}

func NewConfig() *Config {
	redisURL := os.Getenv("REDIS_URL")
	var redisAddr string
	if redisURL != "" {
		// Remove redis:// prefix if present
		redisAddr = strings.TrimPrefix(redisURL, "redis://")
	} else {
		redisAddr = "localhost:6379"
	}

	return &Config{
		RedisURL: redisAddr,
		Workers:  20,
		Port:     8080,
	}
}
