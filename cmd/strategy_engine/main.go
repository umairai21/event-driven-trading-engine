package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

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

	// 2. Ensure the "ORDERS" stream exists for us to publish to later
	setupOrdersStream()

	log.Println("🧠 Strategy Engine is running. Listening for market data...")

	// 3. Subscribe to the Market Data Stream
	// We use a "Durable" name so NATS remembers where we left off if this service crashes
	sub, err := broker.JS.Subscribe("MARKET.prices.*", processMarketTick, nats.Durable("strategy-consumer"))
	if err != nil {
		log.Fatalf("❌ Failed to subscribe to market data: %v", err)
	}
	defer sub.Unsubscribe()

	// 4. Keep the service running until we press Ctrl+C
	keepAlive()
}

// processMarketTick is triggered instantly every time a new price hits NATS
func processMarketTick(msg *nats.Msg) {
	var tick models.PriceTick

	// 1. Unmarshal the JSON back into our Go struct
	if err := json.Unmarshal(msg.Data, &tick); err != nil {
		log.Printf("⚠️ Error decoding message: %v", err)
		return
	}

	// 2. The "Trading Algorithm" (Very simple for now)
	// Let's say our strategy is: Buy 10 shares if the price drops below $120
	if tick.Price < 120.00 {
		log.Printf("💡 SIGNAL: %s dropped to $%.2f! Initiating BUY order.", tick.Ticker, tick.Price)

		order := models.OrderRequest{
			Ticker:    tick.Ticker,
			Action:    "BUY",
			Price:     tick.Price,
			Quantity:  10,
			Timestamp: tick.Timestamp,
		}

		// Send the order to the Execution Service via NATS
		publishOrder(order)
	}

	// 3. Acknowledge the message! 
	// This tells NATS JetStream: "I successfully processed this, you can remove it from my queue."
	msg.Ack()
}

func publishOrder(order models.OrderRequest) {
	orderJSON, err := json.Marshal(order)
	if err != nil {
		log.Printf("⚠️ Failed to marshal order: %v", err)
		return
	}

	// Publish to the ORDERS stream
	subject := "ORDERS.new"
	_, err = broker.JS.Publish(subject, orderJSON)
	if err != nil {
		log.Printf("⚠️ Failed to publish order: %v", err)
	} else {
		log.Printf("📤 Published BUY order for 10 shares of %s", order.Ticker)
	}
}

// setupOrdersStream ensures the stream for execution requests exists
func setupOrdersStream() {
	streamName := "ORDERS"
	_, err := broker.JS.StreamInfo(streamName)
	if err != nil {
		_, err = broker.JS.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{"ORDERS.*"},
		})
		if err != nil {
			log.Fatalf("❌ Error creating JetStream 'ORDERS': %v", err)
		}
		log.Println("✅ JetStream 'ORDERS' initialized!")
	}
}

// keepAlive prevents the Go application from exiting immediately
func keepAlive() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("\n🛑 Shutting down Strategy Engine...")
}