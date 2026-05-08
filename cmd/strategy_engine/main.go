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
	_ = godotenv.Load()
	if err := database.Connect(); err != nil {
		log.Fatalf("❌ Database connection failed: %v", err)
	}
	if err := broker.Connect(); err != nil {
		log.Fatalf("❌ NATS connection failed: %v", err)
	}

	
	setupOrdersStream()
	setupMarketStream()

	log.Println("🧠 Strategy Engine is running. Listening for market data...")

	sub, err := broker.JS.Subscribe("MARKET.prices.*", processMarketTick, nats.Durable("strategy-consumer"))
	if err != nil {
		log.Fatalf("❌ Failed to subscribe to market data: %v", err)
	}
	defer sub.Unsubscribe()

	keepAlive()
}

func processMarketTick(msg *nats.Msg) {
	var tick models.PriceTick

	if err := json.Unmarshal(msg.Data, &tick); err != nil {
		log.Printf("⚠️ Error decoding message: %v", err)
		return
	}

	if tick.Ticker == "SOLUSDT" && tick.Price < 250.00 {
		log.Printf("💡 SIGNAL: %s dropped to $%.2f! Initiating BUY order.", tick.Ticker, tick.Price)

		order := models.OrderRequest{
			Ticker:    tick.Ticker,
			Action:    "BUY",
			Price:     tick.Price,
			Quantity:  10,
			Timestamp: tick.Timestamp,
		}

		publishOrder(order)
	}

	msg.Ack()
}

func publishOrder(order models.OrderRequest) {
	orderJSON, err := json.Marshal(order)
	if err != nil {
		log.Printf("⚠️ Failed to marshal order: %v", err)
		return
	}

	subject := "ORDERS.new"
	_, err = broker.JS.Publish(subject, orderJSON)
	if err != nil {
		log.Printf("⚠️ Failed to publish order: %v", err)
	} else {
		log.Printf("📤 Published BUY order for 10 shares of %s", order.Ticker)
	}
}

func setupMarketStream() {
	streamName := "MARKET"
	_, err := broker.JS.StreamInfo(streamName)
	if err != nil {
		_, err = broker.JS.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{"MARKET.prices.*"},
		})
		if err != nil {
			log.Fatalf("❌ Error creating JetStream 'MARKET': %v", err)
		}
		log.Println("✅ JetStream 'MARKET' initialized by Strategy Engine!")
	}
}

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

func keepAlive() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("\n🛑 Shutting down Strategy Engine...")
}