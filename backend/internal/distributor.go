package internal

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
	paymentChan chan Payment
	store       *Store
	workers     int
	client      *http.Client
}

func NewPaymentProcessor(workers int, store *Store) *PaymentProcessor {
	paymentChan := make(chan Payment, 1000)

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}
	processor := &PaymentProcessor{
		paymentChan: paymentChan,
		workers:     workers,
		store:       store,
		client:      httpClient,
	}

	for range workers {
		go processor.distributePayment(paymentChan)
	}

	return processor
}

func (p *PaymentProcessor) distributePayment(paymentChan chan Payment) {
	for payment := range paymentChan {
		ctx := context.Background()
		respCode, err := p.sendPaymentToProcessor(MAIN_PAYMENT_PROCESSOR_URL, payment)
		if err != nil {
			log.Printf("error sending request to primary processor: %v", err)
			respCode, err = p.sendPaymentToProcessor(SECONDARY_PAYMENT_PROCESSOR_URL, payment)
			if err != nil {
				log.Printf("error sending request to secondary processor: %v", err)
				continue
			}
			p.store.IncrementSummary(ctx, payment.Amount, "fallback")
			continue
		}

		switch respCode {
		case http.StatusOK:
			p.store.IncrementSummary(ctx, payment.Amount, "default")
		case http.StatusTooManyRequests:
			_, err := p.sendPaymentToProcessor(SECONDARY_PAYMENT_PROCESSOR_URL, payment)
			if err != nil {
				log.Printf("error sending request to secondary processor: %v", err)
			} else {
				p.store.IncrementSummary(ctx, payment.Amount, "fallback")
			}
		default:
			log.Printf("unexpected response code from primary processor: %d", respCode)
		}
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

	if resp.StatusCode == http.StatusTooManyRequests {
		return http.StatusTooManyRequests, nil
	}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("request failed with status code: %d", resp.StatusCode)
	}

	log.Printf("Payment processed successfully by %s", url)
	return http.StatusOK, nil
}
