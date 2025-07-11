package distributor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

var (
	MAIN_PAYMENT_PROCESSOR_URL      = "http://payment-processor-default:8080/payments"
	SECONDARY_PAYMENT_PROCESSOR_URL = "http://payment-processor-fallback:8080/payments"
	MAIN_HEALTH_URL                 = "http://payment-processor-default:8080/payments/service-health"
	SECONDARY_HEALTH_URL            = "http://payment-processor-fallback:8080/payments/service-health"
)

type PaymentProcessor struct {
	store   *Store
	workers int
	client  *http.Client
}

func NewPaymentProcessor(workers int, store *Store) *PaymentProcessor {

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
		store:   store,
		client:  httpClient,
	}

	for range workers {
		go processor.distributePayment()
	}

	return processor
}

func (p *PaymentProcessor) distributePayment() {

	ctx := context.Background()
	workerID := fmt.Sprintf("worker-%d", time.Now().UnixNano())

	processingQueue := "payments:processing:" + workerID
	sourceQueue := "payments:queue"

	for {
		// move para fila de processamento para nao perder caso ocorra crash
		res, err := p.store.redisClient.RPopLPush(ctx, sourceQueue, processingQueue).Result()
		if err != nil {
			if err.Error() != "redis: nil" {
				log.Printf("❌ Failed to pop from Redis: %v", err)
			}
			time.Sleep(1 * time.Second)
			continue
		}

		var payment Payment
		if err := json.Unmarshal([]byte(res), &payment); err != nil {
			log.Printf("⚠️ Failed to unmarshal payment: %v", err)
			continue
		}

		respCode, err := p.sendPaymentToProcessor(MAIN_PAYMENT_PROCESSOR_URL, payment)
		if err != nil {
			log.Printf("❌ Error sending payment to main processor: %v", err)
		}
		if respCode == http.StatusOK && respCode < 300 {
			p.store.StorePayment(ctx, payment, "default", payment.ReceivedAt)
			p.store.IncrementSummary(ctx, payment, "default")
		} else {
			log.Printf("⚠️ Main processor failed with status %d, trying fallback", respCode)
			respCode, err = p.sendPaymentToProcessor(SECONDARY_PAYMENT_PROCESSOR_URL, payment)
			if err != nil {
				log.Printf("❌ Error sending payment to fallback processor: %v", err)
			}
			if respCode == http.StatusOK {
				p.store.StorePayment(ctx, payment, "fallback", payment.ReceivedAt)
				p.store.IncrementSummary(ctx, payment, "fallback")
			}
		}
		p.store.redisClient.LRem(context.Background(), processingQueue, 0, res)
	}
}

func (p *PaymentProcessor) sendPaymentToProcessor(url string, payment Payment) (int, error) {
	jsonBody, err := json.Marshal(payment)
	if err != nil {
		return 0, fmt.Errorf("error transforming payment to json: %w", err)
	}

	bodyReader := bytes.NewReader(jsonBody)
	req, err := http.NewRequest(http.MethodPost, url, bodyReader)
	if err != nil {
		return 0, fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error sending request to %s: %w", url, err)
	}

	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("received non-2xx response: %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}
