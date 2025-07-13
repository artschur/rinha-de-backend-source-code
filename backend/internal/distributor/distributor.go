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
	MAIN_PAYMENT_PROCESSOR_URL      = "http://payment-processor-default:8080/payments/"
	SECONDARY_PAYMENT_PROCESSOR_URL = "http://payment-processor-fallback:8080/payments/"
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
			MaxIdleConns:        10, // Reduced from 100
			MaxIdleConnsPerHost: 5,  // Reduced from 20
			IdleConnTimeout:     30 * time.Second,
			DisableKeepAlives:   false,
		},
	}

	processor := &PaymentProcessor{
		workers: workers,
		Store:   store,
		client:  httpClient,
		healthyProcessor: &PaymentProcessorDestination{
			URL:        MAIN_PAYMENT_PROCESSOR_URL,
			Service:    "default",
			HEALTH_URL: MAIN_HEALTH_URL,
		},
	}

	processor.healthyProcessor = &PaymentProcessorDestination{}
	for i := 0; i < workers; i++ {
		go processor.distributePayment(i)
	}

	go processor.automatedHealthCheck()
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

		if err := p.ProccessPayments(payment); err != nil {
			fmt.Printf("[Worker %s] Failed to process payment: %v\n", workerNum, err)
		} else {
			fmt.Printf("[Worker %s] Payment processed: %s\n", workerNum, payment.CorrelationId)
		}

	}
}

func (p *PaymentProcessor) ProccessPayments(paymentRequest models.PaymentRequest) (err error) {
	requestBody, err := json.Marshal(paymentRequest)
	if err != nil {
		return err
	}

	resp, err := p.client.Post(p.healthyProcessor.URL, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("failed to send payment request to processor %v: %w", p.healthyProcessor.Service, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("error sending to payment processor %s", p.healthyProcessor.Service)
	}

	timeSent := time.Now().UTC()
	processedPayment := models.Payment{
		PaymentRequest: paymentRequest,
		Service:        p.healthyProcessor.Service,
		ReceivedAt:     timeSent,
	}

	err = p.Store.StorePayment(context.Background(), processedPayment)
	if err != nil {
		return fmt.Errorf("failed to save payment in redis: %w", err)
	}

	log.Println("PAYMENT SAVED ON REDIS")
	return nil
}

func (p *PaymentProcessor) automatedHealthCheck() {
	for {
		healthyProcessor := p.getHealthyProcessor()
		if healthyProcessor != nil {
			p.healthyProcessor = healthyProcessor
		} else {
			p.healthyProcessor = nil
		}
		time.Sleep(5 * time.Second)
	}
}

func (p *PaymentProcessor) getHealthyProcessor() *PaymentProcessorDestination {
	if p.healthyProcessor != nil {
		return p.healthyProcessor
	}

	if p.checkHealth(MAIN_HEALTH_URL) {
		p.healthyProcessor = &PaymentProcessorDestination{
			URL:        MAIN_PAYMENT_PROCESSOR_URL,
			Service:    "default",
			HEALTH_URL: MAIN_HEALTH_URL,
		}
		return p.healthyProcessor
	}

	if p.checkHealth(SECONDARY_HEALTH_URL) {
		p.healthyProcessor = &PaymentProcessorDestination{
			URL:        SECONDARY_PAYMENT_PROCESSOR_URL,
			Service:    "fallback",
			HEALTH_URL: SECONDARY_HEALTH_URL,
		}
		return p.healthyProcessor
	}

	return nil
}

func (p *PaymentProcessor) checkHealth(url string) bool {
	resp, err := p.client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	var healthCheckResponse models.HealthCheckResponse
	err = json.NewDecoder(resp.Body).Decode(&healthCheckResponse)
	if err != nil {
		return false
	}
	if !healthCheckResponse.Failing {
		return false
	}

	return true
}
