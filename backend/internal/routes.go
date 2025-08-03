package internal

import (
	"context"
	"encoding/json"

	"rinha-backend-arthur/internal/distributor"
	"rinha-backend-arthur/internal/health"
	"rinha-backend-arthur/internal/models"
	"rinha-backend-arthur/internal/store"
	"time"

	"github.com/fasthttp/router"
	"github.com/redis/go-redis/v9"
	"github.com/valyala/fasthttp"
)

func CreateRouter(router *router.Router, config Config) {

	redisClient := redis.NewClient(&redis.Options{
		Addr:     config.RedisURL,
		Password: "",
		DB:       0,
		PoolSize: 50,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		// log.Printf("Warning: Redis connection failed: %v", err)
	}

	store := &store.Store{
		RedisClient: redisClient,
	}

	healthCheckService := health.NewHealthCheckService(store)

	newProcessor := distributor.NewPaymentProcessor(config.Workers, store, healthCheckService)
	handler := &Handler{paymentProcessor: newProcessor}

	router.POST("/payments", handler.HandlePayments)
	router.GET("/payments-summary", handler.HandlePaymentsSummary)
	router.POST("/purge-payments", handler.HandlePurgePayments)
}

type Handler struct {
	paymentProcessor *distributor.PaymentProcessor
}

func (h *Handler) HandlePayments(ctx *fasthttp.RequestCtx) {
	payload := append([]byte(nil), ctx.PostBody()...)
	err := h.paymentProcessor.Store.RedisClient.LPush(context.Background(), "payments:queue", payload).Err()
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("Failed to enqueue payment")
		return
	}

	ctx.SetStatusCode(fasthttp.StatusAccepted)
}

func (h *Handler) HandlePaymentsSummary(ctx *fasthttp.RequestCtx) {
	fromStr := string(ctx.QueryArgs().Peek("from"))
	toStr := string(ctx.QueryArgs().Peek("to"))

	from, to, err := parseTimeRange(fromStr, toStr)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString(err.Error())
		return
	}

	var summary models.PaymentSummary
	if !from.IsZero() && !to.IsZero() {
		summary, err = h.paymentProcessor.Store.GetPaymentSummaryByTimePipeline(ctx, from, to)
	} else {
		summary, err = h.paymentProcessor.Store.GetPaymentSummaryByTimePipeline(ctx, time.Unix(0, 0), time.Unix(1<<63-1, 0))
	}
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("Failed to retrieve payment summary")
		return
	}

	response := models.PaymentSummaryResponse{
		Default: models.SummaryResponse{
			TotalRequests: summary.Default.TotalRequests,
			TotalAmount:   float64(summary.Default.TotalAmount) / 100.0,
		},
		Fallback: models.SummaryResponse{
			TotalRequests: summary.Fallback.TotalRequests,
			TotalAmount:   float64(summary.Fallback.TotalAmount) / 100.0,
		},
	}

	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	if data, err := json.Marshal(response); err == nil {
		ctx.SetBody(data)
	} else {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("Failed to encode summary")
	}
}
func (h *Handler) HandlePurgePayments(ctx *fasthttp.RequestCtx) {
	// Use context.Background() or create a context if needed
	err := h.paymentProcessor.Store.PurgeAllData(context.Background())
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("Failed to purge payment data")
		return
	}

	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)

	response := map[string]string{
		"status":  "success",
		"message": "Payment data purged successfully",
	}

	if data, err := json.Marshal(response); err == nil {
		ctx.SetBody(data)
	} else {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
	}
}
