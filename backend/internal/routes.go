package internal

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"rinha-backend-arthur/internal/distributor"
	"rinha-backend-arthur/internal/models"
	"rinha-backend-arthur/internal/store"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func CreateRouter(mux *http.ServeMux, config Config) {

	redisClient := redis.NewClient(&redis.Options{
		Addr:     config.RedisURL,
		Password: "",
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Redis connection failed: %v", err)
	}

	store := &store.Store{
		RedisClient: redisClient,
	}

	newProcessor := distributor.NewPaymentProcessor(config.Workers, store)
	handler := &Handler{paymentProcessor: newProcessor}

	mux.HandleFunc("POST /payments", handler.HandlePayments)
	mux.HandleFunc("GET /payments-summary", handler.HandlePaymentsSummary)
	mux.HandleFunc("POST /purge-payments", handler.HandlePurgePayments)
}

type Handler struct {
	paymentProcessor *distributor.PaymentProcessor
}

func (h *Handler) HandlePayments(w http.ResponseWriter, r *http.Request) {
	var paymentRequest models.PaymentRequest
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

	payment := models.Payment{
		PaymentRequest: paymentRequest,
		ReceivedAt:     time.Now().UTC(),
	}

	payload, err := json.Marshal(payment)
	if err != nil {
		log.Printf("Error marshalling payment request: %v", err)
		http.Error(w, "Failed to process payment", http.StatusInternalServerError)
		return
	}

	if err := h.paymentProcessor.Store.RedisClient.LPush(r.Context(), "payments:queue", payload).Err(); err != nil {
		log.Printf("Error pushing payment to Redis: %v", err)
		http.Error(w, "Failed to process payment", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	response := map[string]string{
		"status":        "success",
		"message":       "Payment request accepted",
		"correlationId": paymentRequest.CorrelationId.String(),
	}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

func (h *Handler) HandlePaymentsSummary(w http.ResponseWriter, r *http.Request) {

	payments, err := h.paymentProcessor.Store.GetAllPayments(r.Context())
	if err != nil {
		log.Printf("Error retrieving payments: %v", err)
		http.Error(w, "Failed to retrieve payments", http.StatusInternalServerError)
		return
	}

	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")

	fromTime, err := ParseFlexibleTime(from)
	if err != nil {
		log.Printf("Error parsing 'from' time: %v", err)
		http.Error(w, "Invalid 'from' time format", http.StatusBadRequest)
		return
	}
	toTime, err := ParseFlexibleTime(to)
	if err != nil {
		log.Printf("Error parsing 'to' time: %v", err)
		http.Error(w, "Invalid 'to' time format", http.StatusBadRequest)
		return
	}

	summary := PaymentsToSummary(payments, fromTime, toTime)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(summary); err != nil {
		log.Printf("Error encoding summary response: %v", err)
		http.Error(w, "Failed to encode summary", http.StatusInternalServerError)
	}
}

func (h *Handler) HandlePurgePayments(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Delete all payment summary data from Redis
	err := h.paymentProcessor.Store.PurgeAllData(ctx)
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
