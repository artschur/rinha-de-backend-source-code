package health

import (
	"context"
	"encoding/json"
	"net/http"
	"rinha-backend-arthur/internal/models"
	"rinha-backend-arthur/internal/store"
	"time"
)

type HealthCheckService struct {
	store            *store.Store
	HealthyProcessor *PaymentProcessorDestination
}

type PaymentProcessorDestination struct {
	URL        string
	HEALTH_URL string // health check URL
	Service    string // default or fallback
}

var (
	DEFAULT_PAYMENT_PROCESSOR_URL  = "http://payment-processor-default:8080/payments"
	FALLBACK_PAYMENT_PROCESSOR_URL = "http://payment-processor-fallback:8080/payments"
	DEFAULT_HEALTH_URL             = "http://payment-processor-default:8080/payments/service-health"
	FALLBACK_HEALTH_URL            = "http://payment-processor-fallback:8080/payments/service-health"
)

func NewHealthCheckService(store *store.Store) *HealthCheckService {
	return &HealthCheckService{
		store: store,
		HealthyProcessor: &PaymentProcessorDestination{
			URL:        DEFAULT_PAYMENT_PROCESSOR_URL,
			HEALTH_URL: DEFAULT_HEALTH_URL,
			Service:    "default",
		},
	}
}

func (h *HealthCheckService) StartHealthCheckLoop() {
	ticker := time.NewTicker(6 * time.Second) // Slightly longer than rate limit
	defer ticker.Stop()

	for range ticker.C {
		// Try to acquire lock for health check
		if h.acquireHealthCheckLock() {
			// log.Printf("🔐 Acquired health check lock, performing health checks...")
			h.updateHealthyProcessorWithRedis()
			h.releaseHealthCheckLock()
		} else {
			// log.Printf("📖 Another replica is doing health checks, reading status from Redis...")
			h.readHealthStatusFromRedis()
		}
	}
}

func (h *HealthCheckService) isHealthy(url string) bool {

	resp, err := http.Get(url)
	if err != nil {
		// log.Printf("Health check request failed for %s: %v", url, err)
		return false
	}
	defer resp.Body.Close()

	var healthCheckResponse models.HealthCheckResponse
	err = json.NewDecoder(resp.Body).Decode(&healthCheckResponse)
	if err != nil {
		// log.Printf("Failed to decode health response from %s: %v", url, err)
		return false
	}

	if healthCheckResponse.Failing {
		return false
	}

	return true
}

func (h *HealthCheckService) acquireHealthCheckLock() bool {
	ctx := context.Background()
	// Try to set lock with 10 second expiration (in case process crashes)
	result := h.store.RedisClient.SetNX(ctx, "health_check_lock", "locked", 10*time.Second)
	acquired := result.Val()
	if acquired {
		// log.Printf("✅ Health check lock acquired")
	}
	return acquired
}

func (h *HealthCheckService) releaseHealthCheckLock() {
	ctx := context.Background()
	h.store.RedisClient.Del(ctx, "health_check_lock")
	// log.Printf("🔓 Health check lock released")
}

func (h *HealthCheckService) updateHealthyProcessorWithRedis() {
	// log.Printf("=== Starting health check cycle ===")

	// Check main processor first
	mainHealthy := h.isHealthy(DEFAULT_HEALTH_URL)
	// log.Printf("Main processor health: %v", mainHealthy)

	if mainHealthy {
		newProcessor := &PaymentProcessorDestination{
			URL:     DEFAULT_PAYMENT_PROCESSOR_URL,
			Service: "default",
		}
		if h.HealthyProcessor.Service != "default" {
			// log.Printf("🔄 Switching to main processor (default)")
		}
		h.HealthyProcessor = newProcessor
		h.storeHealthStatusInRedis("default")
		return
	}

	// Check fallback processor
	fallbackHealthy := h.isHealthy(FALLBACK_HEALTH_URL)
	// log.Printf("Fallback processor health: %v", fallbackHealthy)

	if fallbackHealthy {
		newProcessor := &PaymentProcessorDestination{
			URL:     FALLBACK_PAYMENT_PROCESSOR_URL,
			Service: "fallback",
		}
		if h.HealthyProcessor.Service != "fallback" {
			// log.Printf("🔄 Switching to fallback processor")
		}
		h.HealthyProcessor = newProcessor
		h.storeHealthStatusInRedis("fallback")
		return
	}

	// Both are down, keep current but update timestamp
	// log.Printf("⚠️  WARNING: Both processors are down, keeping current: %s", h.HealthyProcessor.Service)
	h.storeHealthStatusInRedis(h.HealthyProcessor.Service)
	// log.Printf("=== End health check cycle ===")
}

func (h *HealthCheckService) storeHealthStatusInRedis(service string) {
	ctx := context.Background()
	healthData := map[string]any{
		"service":   service,
		"timestamp": time.Now().Unix(),
	}

	err := h.store.RedisClient.HMSet(ctx, "healthy_processor_status", healthData).Err()
	if err != nil {
		// log.Printf("❌ Failed to store health status in Redis: %v", err)
	} else {
		// log.Printf("💾 Stored health status in Redis: %s", service)
	}
}

func (h *HealthCheckService) readHealthStatusFromRedis() {
	ctx := context.Background()
	result := h.store.RedisClient.HGetAll(ctx, "healthy_processor_status")

	healthData, err := result.Result()
	if err != nil {
		// log.Printf("❌ Failed to read health status from Redis: %v", err)
		return
	}

	if len(healthData) == 0 {
		// log.Printf("📖 No health status found in Redis, keeping current: %s", h.HealthyProcessor.Service)
		return
	}

	service := healthData["service"]
	// timestamp := healthData["timestamp"]

	// log.Printf("📖 Read health status from Redis: service=%s, timestamp=%s", service, timestamp)

	if service == "default" && h.HealthyProcessor.Service != "default" {
		// log.Printf("🔄 Updating to main processor based on Redis status")
		h.HealthyProcessor = &PaymentProcessorDestination{
			URL:     DEFAULT_PAYMENT_PROCESSOR_URL,
			Service: "default",
		}
	} else if service == "fallback" && h.HealthyProcessor.Service != "fallback" {
		// log.Printf("🔄 Updating to fallback processor based on Redis status")
		h.HealthyProcessor = &PaymentProcessorDestination{
			URL:     FALLBACK_PAYMENT_PROCESSOR_URL,
			Service: "fallback",
		}
	}
}
