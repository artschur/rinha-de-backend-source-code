package internal

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func CreateRouter(mux *http.ServeMux) {
	// Handle Redis URL from environment
	redisURL := os.Getenv("REDIS_URL")
	var redisAddr string

	if redisURL != "" {
		// Remove redis:// prefix if present
		redisAddr = strings.TrimPrefix(redisURL, "redis://")
	} else {
		redisAddr = "localhost:6379"
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})

	// Test Redis connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Redis connection failed: %v", err)
	}

	store := &Store{redisClient}
	newProcessor := NewPaymentProcessor(100, store)
	handler := &Handler{paymentProcessor: newProcessor}

	mux.HandleFunc("POST /payments", handler.HandlePayments)
	mux.HandleFunc("GET /payments-summary", handler.HandlePaymentsSummary)
	mux.HandleFunc("POST /purge-payments", handler.HandlePurgePayments)
}

type Handler struct {
	paymentProcessor *PaymentProcessor
}

func (h *Handler) HandlePayments(w http.ResponseWriter, r *http.Request) {
	var paymentRequest Payment
	if err := json.NewDecoder(r.Body).Decode(&paymentRequest); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if paymentRequest.Amount <= 0 {
		http.Error(w, "Amount must be greater than zero", http.StatusBadRequest)
		return
	}
	if paymentRequest.CorrelationId == uuid.Nil {
		http.Error(w, "CorrelationId is required", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	h.paymentProcessor.paymentChan <- paymentRequest
}

func (h *Handler) HandlePaymentsSummary(w http.ResponseWriter, r *http.Request) {
	summary, err := h.paymentProcessor.store.GetSummary(r.Context())
	if err != nil {
		http.Error(w, "Failed to retrieve summary", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(summary); err != nil {
		http.Error(w, "Failed to encode summary", http.StatusInternalServerError)
		return
	}
}

func (h *Handler) HandlePurgePayments(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Delete all payment summary data from Redis
	err := h.paymentProcessor.store.PurgeAllData(ctx)
	if err != nil {
		log.Printf("Error purging payment data: %v", err)
		http.Error(w, "Failed to purge payment data", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]string{
		"status":  "success",
		"message": "Payment data purged successfully",
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding purge response: %v", err)
	}
}
