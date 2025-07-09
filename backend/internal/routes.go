package internal

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
)

func CreateRouter(mux *http.ServeMux) {
	store := &Store{} // Initialize your store here, e.g., with a Redis client
	handler := &Handler{store: store}

	mux.HandleFunc("POST /payments", handler.HandlePayments)
	mux.HandleFunc("GET /payments-summary", handler.HandlePaymentsSummary)
}

type Handler struct {
	store *Store
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

	go h.store.IncrementSummary(r.Context(), paymentRequest.Amount)
	go distributePayment(paymentRequest)

}

func (h *Handler) HandlePaymentsSummary(w http.ResponseWriter, r *http.Request) {
	summary, err := h.store.GetSummary(r.Context())
	if err != nil {
		http.Error(w, "Failed to retrieve summary", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(summary); err != nil {
		http.Error(w, "Failed to encode summary", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Summary retrieved successfully"))
}

func distributePayment(payment Payment) {
	// Logic to distribute payment to the appropriate service
	// This could involve sending the payment to a message queue or another service
}
