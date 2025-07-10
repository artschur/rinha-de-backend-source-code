package main

import (
	"log"
	"net/http"

	"rinha-backend-arthur/internal"
)

func main() {
	mux := http.NewServeMux()

	internal.CreateRouter(mux)

	log.Printf("Starting server on port %v", 9999)
	if err := http.ListenAndServe(":"+"9999", mux); err != nil {
		log.Fatal("Server failed to start:", err)
	}
}
