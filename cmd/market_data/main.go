package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/umairai21/event-driven-trading-engine/internal/broker"
	"github.com/umairai21/event-driven-trading-engine/internal/database"
	"github.com/umairai21/event-driven-trading-engine/internal/models"
)

func main() {
	// 1. Initialize Infrastructure
	_ = godotenv.Load()
	if err := database.Connect(); err != nil {
		log.Fatalf("❌ Database connection failed: %v", err)
	}
	if err := broker.Connect(); err != nil {
		log.Fatalf("❌ NATS connection failed: %v", err)
	}

	// 2. Configure NATS JetStream
	setupJetStream()

	log.Println("📈 Market Data Service is running. Publishing prices...")

	// 3. The Infinite Publishing Loop
	tickers := []string{"AAPL", "TSLA", "GOOG"}

	for {
		for _, ticker := range tickers {
			// Simulate a realistic-ish price using math/rand
			simulatedPrice := 100.0 + (rand.Float64() * 50.0) 

			tick := models.PriceTick{
				Ticker:    ticker,
				Price:     simulatedPrice,
				Timestamp: time.Now(),
			}

			// Convert our Go Struct into JSON bytes
			jsonData, err := json.Marshal(tick)
			if err != nil {
				log.Printf("⚠️ Failed to marshal JSON: %v", err)
				continue
			}

			// Publish to JetStream on a specific "Subject" (e.g., MARKET.prices.AAPL)
			subject := "MARKET.prices." + ticker
			_, err = broker.JS.Publish(subject, jsonData)
			if err != nil {
				log.Printf("⚠️ Failed to publish to NATS: %v", err)
			} else {
				log.Printf("📡 Published %s: $%.2f", ticker, simulatedPrice)
			}
		}

		// Wait 2 seconds before the next batch of prices
		time.Sleep(2 * time.Second)
	}
}

// setupJetStream ensures the NATS stream exists before we try to publish to it
func setupJetStream() {
	streamName := "MARKET"
	
	// Check if the stream already exists
	_, err := broker.JS.StreamInfo(streamName)
	if err != nil {
		// If it doesn't exist, create it. We tell it to listen for any subject starting with "MARKET.prices."
		_, err = broker.JS.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{"MARKET.prices.*"},
		})
		if err != nil {
			log.Fatalf("❌ Error creating JetStream: %v", err)
		}
		log.Println("✅ JetStream 'MARKET' initialized!")
	}
}