package internal

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func CreateRouter(mux *http.ServeMux) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	store := &Store{redisClient}
	newProcessor := NewPaymentProcessor(100, store)
	handler := &Handler{paymentProcessor: newProcessor}

	mux.HandleFunc("POST /payments", handler.HandlePayments)
	mux.HandleFunc("GET /payments-summary", handler.HandlePaymentsSummary)
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
