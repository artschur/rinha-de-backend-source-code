package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
		processed := false

		// Try primary processor first
		respCode, err := p.sendPaymentToProcessor(MAIN_PAYMENT_PROCESSOR_URL, payment)
		if err == nil {
			switch respCode {
			case http.StatusOK:
				p.store.IncrementSummary(ctx, payment.Amount, "default")
				processed = true
			case http.StatusTooManyRequests:
				// Try fallback for rate limiting
				respCode, err = p.sendPaymentToProcessor(SECONDARY_PAYMENT_PROCESSOR_URL, payment)
				if err == nil && respCode == http.StatusOK {
					p.store.IncrementSummary(ctx, payment.Amount, "fallback")
					processed = true
				}
			case http.StatusUnprocessableEntity: // 422 - business rule error
				log.Printf("Payment rejected by primary processor: %s", payment.CorrelationId)
				// Try fallback for business rule failures
				respCode, err = p.sendPaymentToProcessor(SECONDARY_PAYMENT_PROCESSOR_URL, payment)
				if err == nil && respCode == http.StatusOK {
					p.store.IncrementSummary(ctx, payment.Amount, "fallback")
					processed = true
				}
			}
		}

		if !processed && err != nil {
			log.Printf("Primary processor failed: %v, trying fallback", err)
			respCode, err = p.sendPaymentToProcessor(SECONDARY_PAYMENT_PROCESSOR_URL, payment)
			if err == nil && respCode == http.StatusOK {
				p.store.IncrementSummary(ctx, payment.Amount, "fallback")
				processed = true
			}
		}

		if !processed {
			log.Printf("Payment failed on both processors: %s", payment.CorrelationId)
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
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusTooManyRequests {
		return http.StatusTooManyRequests, nil
	}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("request failed with status code: %d, body: %s", resp.StatusCode, string(respBody))
	}

	log.Printf("Payment processed successfully by %s", url)
	return http.StatusOK, nil
}
