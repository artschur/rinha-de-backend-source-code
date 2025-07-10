package main

import (
	"log"
	"net/http"
	"os"

	"rinha-backend-arthur/internal"
)

func main() {
	mux := http.NewServeMux()

	internal.CreateRouter(mux)
	port := os.Getenv("PORT")
	log.Printf("Starting server on port %v", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal("Server failed to start:", err)
	}
}
