package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rinha-backend-arthur/internal"
)

func main() {
	config := internal.NewConfig()

	mux := http.NewServeMux()
	internal.CreateRouter(mux, *config)

	server := &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		// log.Println("Server starting on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// log.Fatalf("Could not listen on :8080: %v\n", err)
		}
	}()

	<-stop
	// log.Println("Shutting down server...")

	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		// log.Fatalf("Server forced to shutdown: %v", err)
	}

	// log.Println("Server exited properly")
}
