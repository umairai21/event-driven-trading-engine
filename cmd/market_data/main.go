package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"github.com/umairai21/event-driven-trading-engine/internal/broker"
	"github.com/umairai21/event-driven-trading-engine/internal/database"
	"github.com/umairai21/event-driven-trading-engine/internal/models"
)


type BinancePrice struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"` 
}

func main() {
	_ = godotenv.Load()
	if err := database.Connect(); err != nil {
		log.Fatalf("❌ Database connection failed: %v", err)
	}
	if err := broker.Connect(); err != nil {
		log.Fatalf("❌ NATS connection failed: %v", err)
	}

	setupJetStream()

	maxIterations := 15
	log.Printf("📈 LIVE Market Data Service running. Fetching from Binance %d times...", maxIterations)

	client := &http.Client{Timeout: 10 * time.Second}

	for i := 1; i <= maxIterations; i++ {
		log.Printf("--- Fetch Cycle %d of %d ---", i, maxIterations)
		fetchAndPublishPrices(client)
		time.Sleep(5 * time.Second)
	}

	log.Println("🛑 Reached API cap limit. Market Data Service shutting down cleanly.")
}

func fetchAndPublishPrices(client *http.Client) {
	url := `https://api.binance.com/api/v3/ticker/price?symbols=["BTCUSDT","ETHUSDT","SOLUSDT"]`
	
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("⚠️ Failed to create request: %v", err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("⚠️ API request failed: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("⚠️ API Error: Received HTTP Status %d", resp.StatusCode)
		return
	}

	var binanceData []BinancePrice
	if err := json.NewDecoder(resp.Body).Decode(&binanceData); err != nil {
		log.Printf("⚠️ Failed to parse API JSON: %v", err)
		return
	}

	for _, item := range binanceData {
		priceFloat, err := strconv.ParseFloat(item.Price, 64)
		if err != nil {
			continue
		}

		tick := models.PriceTick{
			Ticker:    item.Symbol,
			Price:     priceFloat,
			Timestamp: time.Now(),
		}

		jsonData, err := json.Marshal(tick)
		if err != nil {
			continue
		}

		subject := "MARKET.prices." + item.Symbol
		_, err = broker.JS.Publish(subject, jsonData)
		if err != nil {
			log.Printf("⚠️ Failed to publish to NATS: %v", err)
		} else {
			log.Printf("📡 REALTIME Published %s: $%.2f", item.Symbol, priceFloat)
		}
	}
}

func setupJetStream() {
	streamName := "MARKET"
	_, err := broker.JS.StreamInfo(streamName)
	if err != nil {
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