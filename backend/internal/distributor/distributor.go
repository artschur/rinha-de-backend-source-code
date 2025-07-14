package distributor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"rinha-backend-arthur/internal/models"
	"rinha-backend-arthur/internal/store"
	"time"
)

type PaymentProcessorDestination struct {
	URL        string
	HEALTH_URL string // health check URL
	Service    string // default or fallback
}

var (
	MAIN_PAYMENT_PROCESSOR_URL      = "http://payment-processor-default:8080/payments"
	SECONDARY_PAYMENT_PROCESSOR_URL = "http://payment-processor-fallback:8080/payments"
	MAIN_HEALTH_URL                 = "http://payment-processor-default:8080/payments/service-health"
	SECONDARY_HEALTH_URL            = "http://payment-processor-fallback:8080/payments/service-health"
)

type PaymentProcessor struct {
	Store            *store.Store
	workers          int
	client           *http.Client
	healthyProcessor *PaymentProcessorDestination
}

func NewPaymentProcessor(workers int, store *store.Store) *PaymentProcessor {
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 5,
			IdleConnTimeout:     30 * time.Second,
			DisableKeepAlives:   false,
		},
	}

	processor := &PaymentProcessor{
		workers: workers,
		Store:   store,
		client:  httpClient,
		// Always start with main processor
		healthyProcessor: &PaymentProcessorDestination{
			URL:     MAIN_PAYMENT_PROCESSOR_URL,
			Service: "default",
		},
	}

	// Start health check with ticker
	go processor.startHealthCheckLoop()

	for i := 0; i < workers; i++ {
		go processor.distributePayment(i)
	}

	return processor
}

func (p *PaymentProcessor) distributePayment(workerNum int) {

	if p.Store == nil || p.Store.RedisClient == nil {
		log.Fatal("PaymentProcessor store or RedisClient is nil!")
	}

	ctx := context.Background()
	for {
		result, err := p.Store.RedisClient.RPop(ctx, "payments:queue").Result()
		if err != nil {
			if err.Error() == "redis: nil" {
				// no payments in the queue, wait before checking again
				time.Sleep(1 * time.Second)
				continue
			}
			// Handle other errors (e.g., connection issues)
			continue
		}

		var payment models.PaymentRequest
		if err := json.Unmarshal([]byte(result), &payment); err != nil {
			fmt.Printf("[Worker %s-%d] Failed to unmarshal payment: %v\n", workerNum, workerNum, err)
			continue
		}

		if err := p.ProcessPayments(payment); err != nil {
			fmt.Printf("[Worker %s] Failed to process payment: %v\n", workerNum, err)
		} else {
			fmt.Printf("[Worker %s] Payment processed: %s\n", workerNum, payment.CorrelationId)
		}

	}
}

func (p *PaymentProcessor) ProcessPayments(paymentRequest models.PaymentRequest) error {
	// Just marshal the struct directly - perfect JSON!
	requestBody, err := json.Marshal(paymentRequest)
	if err != nil {
		return err
	}

	resp, err := p.client.Post(p.healthyProcessor.URL, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("failed to send payment request to processor %s: %w", p.healthyProcessor.Service, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("error sending to payment processor %s: status %d", p.healthyProcessor.Service, resp.StatusCode)
	}

	// Create payment record with same timestamp
	processedPayment := models.Payment{
		PaymentRequest: paymentRequest, // This includes RequestedAt!
		Service:        p.healthyProcessor.Service,
	}

	err = p.Store.StorePayment(context.Background(), processedPayment)
	if err != nil {
		return fmt.Errorf("failed to save payment in redis: %w", err)
	}

	return nil
}

func (p *PaymentProcessor) checkHealth(url string) bool {
	log.Printf("Checking health for: %s", url)

	resp, err := p.client.Get(url)
	if err != nil {
		log.Printf("Health check request failed for %s: %v", url, err)
		return false
	}
	defer resp.Body.Close()

	var healthCheckResponse models.HealthCheckResponse
	err = json.NewDecoder(resp.Body).Decode(&healthCheckResponse)
	if err != nil {
		log.Printf("Failed to decode health response from %s: %v", url, err)
		return false
	}

	if !healthCheckResponse.Failing {
		log.Printf("Health check for %s: failing=false, considering unhealthy", url)
		return true
	}

	return false
}

func (p *PaymentProcessor) startHealthCheckLoop() {
	ticker := time.NewTicker(6 * time.Second) // Slightly longer than rate limit
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Try to acquire lock for health check
			if p.acquireHealthCheckLock() {
				log.Printf("🔐 Acquired health check lock, performing health checks...")
				p.updateHealthyProcessorWithRedis()
				p.releaseHealthCheckLock()
			} else {
				log.Printf("📖 Another replica is doing health checks, reading status from Redis...")
				p.readHealthStatusFromRedis()
			}
		}
	}
}

func (p *PaymentProcessor) acquireHealthCheckLock() bool {
	ctx := context.Background()
	// Try to set lock with 10 second expiration (in case process crashes)
	result := p.Store.RedisClient.SetNX(ctx, "health_check_lock", "locked", 10*time.Second)
	acquired := result.Val()
	if acquired {
		log.Printf("✅ Health check lock acquired")
	}
	return acquired
}

func (p *PaymentProcessor) releaseHealthCheckLock() {
	ctx := context.Background()
	p.Store.RedisClient.Del(ctx, "health_check_lock")
	log.Printf("🔓 Health check lock released")
}

func (p *PaymentProcessor) updateHealthyProcessorWithRedis() {
	log.Printf("=== Starting health check cycle ===")

	// Check main processor first
	mainHealthy := p.checkHealth(MAIN_HEALTH_URL)
	log.Printf("Main processor health: %v", mainHealthy)

	if mainHealthy {
		newProcessor := &PaymentProcessorDestination{
			URL:     MAIN_PAYMENT_PROCESSOR_URL,
			Service: "default",
		}
		if p.healthyProcessor.Service != "default" {
			log.Printf("🔄 Switching to main processor (default)")
		}
		p.healthyProcessor = newProcessor
		p.storeHealthStatusInRedis("default")
		return
	}

	// Check fallback processor
	fallbackHealthy := p.checkHealth(SECONDARY_HEALTH_URL)
	log.Printf("Fallback processor health: %v", fallbackHealthy)

	if fallbackHealthy {
		newProcessor := &PaymentProcessorDestination{
			URL:     SECONDARY_PAYMENT_PROCESSOR_URL,
			Service: "fallback",
		}
		if p.healthyProcessor.Service != "fallback" {
			log.Printf("🔄 Switching to fallback processor")
		}
		p.healthyProcessor = newProcessor
		p.storeHealthStatusInRedis("fallback")
		return
	}

	// Both are down, keep current but update timestamp
	log.Printf("⚠️  WARNING: Both processors are down, keeping current: %s", p.healthyProcessor.Service)
	p.storeHealthStatusInRedis(p.healthyProcessor.Service)
	log.Printf("=== End health check cycle ===")
}

func (p *PaymentProcessor) storeHealthStatusInRedis(service string) {
	ctx := context.Background()
	healthData := map[string]interface{}{
		"service":   service,
		"timestamp": time.Now().Unix(),
	}

	err := p.Store.RedisClient.HMSet(ctx, "healthy_processor_status", healthData).Err()
	if err != nil {
		log.Printf("❌ Failed to store health status in Redis: %v", err)
	} else {
		log.Printf("💾 Stored health status in Redis: %s", service)
	}
}

func (p *PaymentProcessor) readHealthStatusFromRedis() {
	ctx := context.Background()
	result := p.Store.RedisClient.HGetAll(ctx, "healthy_processor_status")

	healthData, err := result.Result()
	if err != nil {
		log.Printf("❌ Failed to read health status from Redis: %v", err)
		return
	}

	if len(healthData) == 0 {
		log.Printf("📖 No health status found in Redis, keeping current: %s", p.healthyProcessor.Service)
		return
	}

	service := healthData["service"]
	timestamp := healthData["timestamp"]

	log.Printf("📖 Read health status from Redis: service=%s, timestamp=%s", service, timestamp)

	if service == "default" && p.healthyProcessor.Service != "default" {
		log.Printf("🔄 Updating to main processor based on Redis status")
		p.healthyProcessor = &PaymentProcessorDestination{
			URL:     MAIN_PAYMENT_PROCESSOR_URL,
			Service: "default",
		}
	} else if service == "fallback" && p.healthyProcessor.Service != "fallback" {
		log.Printf("🔄 Updating to fallback processor based on Redis status")
		p.healthyProcessor = &PaymentProcessorDestination{
			URL:     SECONDARY_PAYMENT_PROCESSOR_URL,
			Service: "fallback",
		}
	}
}
