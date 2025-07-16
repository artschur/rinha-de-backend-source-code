package distributor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"rinha-backend-arthur/internal/health"
	"rinha-backend-arthur/internal/models"
	"rinha-backend-arthur/internal/store"
	"time"
)

type PaymentProcessor struct {
	Store   *store.Store
	workers int
	client  *http.Client
	health  *health.HealthCheckService
}

func NewPaymentProcessor(workers int, store *store.Store, healthCheckServie *health.HealthCheckService) *PaymentProcessor {
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
		health:  healthCheckServie,
	}

	// Start health check with ticker
	go processor.health.StartHealthCheckLoop()

	for i := range workers {
		go processor.distributePayment(i)
	}

	return processor
}

func (p *PaymentProcessor) distributePayment(workerNum int) {

	if p.Store == nil || p.Store.RedisClient == nil {
		// log.Fatal("PaymentProcessor store or RedisClient is nil!")
	}

	ctx := context.Background()
	processingQueue := fmt.Sprintf("payments:processing:%d", workerNum)

	for {
		result, err := p.Store.RedisClient.RPopLPush(ctx, "payments:queue", processingQueue).Result()
		if err != nil {
			if err.Error() == "redis: nil" {
				time.Sleep(1 * time.Second)
				continue
			}
			// log.Printf("[Worker %d] Redis error: %v", workerNum, err)
			time.Sleep(1 * time.Second)
			continue
		}

		var payment models.PaymentRequest
		if err := json.Unmarshal([]byte(result), &payment); err != nil {
			fmt.Printf("[Worker %v-%d] Failed to unmarshal payment: %v\n", workerNum, workerNum, err)
			p.Store.RedisClient.LRem(ctx, processingQueue, 1, result)
			continue
		}

		if err := p.ProcessPayments(payment); err != nil {
			fmt.Printf("[Worker %v] Failed to process payment: %v\n", workerNum, err)
			p.Store.RedisClient.LPush(ctx, "payments:queue", result) // Requeue the payment
		} else {
			// Successfully processed - remove from processing queue
			p.Store.RedisClient.LRem(ctx, processingQueue, 1, result)
		}

	}
}

func (p *PaymentProcessor) ProcessPayments(paymentRequest models.PaymentRequest) error {
	// evita que o health checker mude no meio
	currentProcessor := p.health.HealthyProcessor
	if currentProcessor == nil {
		return fmt.Errorf("no healthy processor available")
	}

	requestBody, err := json.Marshal(paymentRequest)
	if err != nil {
		return err
	}

	resp, err := p.client.Post(currentProcessor.URL, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return fmt.Errorf("failed to send payment request to processor %s: %w", currentProcessor.Service, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("error sending to payment processor %s: status %d", currentProcessor.Service, resp.StatusCode)
	}

	processedPayment := models.Payment{
		PaymentRequest: paymentRequest,
		Service:        currentProcessor.Service, // Use captured processor
	}

	err = p.Store.StorePayment(context.Background(), processedPayment)
	if err != nil {
		// This is critical - payment was accepted by processor but we failed to save
		// Log as error but don't return error to avoid reprocessing
		// log.Printf("CRITICAL: Payment accepted by processor but failed to save in Redis: %v", err)
	}

	return nil
}
