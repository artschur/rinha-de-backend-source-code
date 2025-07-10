package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
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
}

func NewPaymentProcessor(workers int) *PaymentProcessor {
	paymentChan := make(chan Payment, 1000)
	processor := &PaymentProcessor{
		paymentChan: paymentChan,
		workers:     workers,
	}

	for range 100 {
		go distributePayment(paymentChan)
	}

	return processor
}

func distributePayment(paymentChan chan Payment) {
	for payment := range paymentChan {
		respCode, err := sendPaymentToProcessor(MAIN_PAYMENT_PROCESSOR_URL, payment)
		if err != nil {
			fmt.Errorf("error distributing payment, %v", err)
		}
		if respCode == http.StatusTooManyRequests {
			sendPaymentToProcessor(SECONDARY_PAYMENT_PROCESSOR_URL, payment)
		}
	}
}

func sendPaymentToProcessor(url string, payment Payment) (code int, error error) {
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
	resp, err := http.DefaultClient.Do(req)
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
