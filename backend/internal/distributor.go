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
			log.Printf("error sending request to primary processor, %v \n", err)
		}
		switch respCode {
		case http.StatusOK:
			p.store.IncrementSummary(ctx, payment.Amount, "default")
		case http.StatusTooManyRequests:
			_, err := p.sendPaymentToProcessor(SECONDARY_PAYMENT_PROCESSOR_URL, payment)
			if err != nil {
				log.Printf("error sending request to secondary processor, %v \n", err)
			}
			p.store.IncrementSummary(ctx, payment.Amount, "fallback")
		}
	}
}

func (p *PaymentProcessor) sendPaymentToProcessor(url string, payment Payment) (code int, error error) {
	jsonBody, err := json.Marshal(payment)
	if err != nil {
		return 0, fmt.Errorf("error transforming payments to json: %v ", err)
	}

	bodyReader := bytes.NewReader(jsonBody)
	req, err := http.NewRequest(http.MethodPost, url, bodyReader)
	if err != nil {
		return 0, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return http.StatusTooManyRequests, nil
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("request couldnt be received, %v", err)
	}
	return http.StatusOK, nil
}
