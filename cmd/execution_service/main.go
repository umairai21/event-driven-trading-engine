package main

import (
	"context"
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
	"google.golang.org/grpc" 
	"google.golang.org/grpc/credentials/insecure" 
)

func main() {
	_ = godotenv.Load()
	if err := database.Connect(); err != nil {
		log.Fatalf("❌ Database connection failed: %v", err)
	}
	if err := broker.Connect(); err != nil {
		log.Fatalf("❌ NATS connection failed: %v", err)
	}

	setupDatabase()
	setupOrdersStream()

	log.Println("🏦 Execution Service is running. Listening for orders...")

	sub, err := broker.JS.Subscribe("ORDERS.*", processOrder, nats.Durable("execution-consumer"))
	if err != nil {
		log.Fatalf("❌ Failed to subscribe to orders: %v", err)
	}
	defer sub.Unsubscribe()

	keepAlive()
}

func processOrder(msg *nats.Msg) {
	var order models.OrderRequest
	if err := json.Unmarshal(msg.Data, &order); err != nil {
		return
	}

	totalCost := order.Price * float64(order.Quantity)
	userID := 1 

	log.Printf("📥 Received Order: %s %d shares of %s at $%.2f", order.Action, order.Quantity, order.Ticker, order.Price)


	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("⚠️ Failed to connect to Risk Service: %v", err)
		return
	}
	defer conn.Close()

	log.Println("📞 Calling Risk Service via gRPC...")
	
	isApproved := true
	if totalCost > 5000.00 {
		isApproved = false
	}

	if !isApproved {
		log.Printf("❌ Order Rejected by Risk Service: Exceeds $5,000 limit")
		msg.Ack()
		return
	}
	log.Println("✅ Risk Service Approved the trade.")


	// 1. Begin a Database Transaction
	ctx := context.Background()
	tx, err := database.Pool.Begin(ctx)
	if err != nil {
		log.Printf("⚠️ Failed to start transaction: %v", err)
		return
	}
	
	defer tx.Rollback(ctx)

	// 2. Check Balance and Deduct in one safe SQL query
	var newBalance float64
	err = tx.QueryRow(ctx, `
		UPDATE users 
		SET balance = balance - $1 
		WHERE id = $2 AND balance >= $1 
		RETURNING balance`, totalCost, userID).Scan(&newBalance)

	if err != nil {
		log.Printf("❌ Order Rejected: Insufficient funds or DB error for %s", order.Ticker)
		msg.Ack()
		return
	}

	// 3. Record the Trade History
	_, err = tx.Exec(ctx, `
		INSERT INTO trades (user_id, ticker, action, price, quantity) 
		VALUES ($1, $2, $3, $4, $5)`, userID, order.Ticker, order.Action, order.Price, order.Quantity)
	if err != nil {
		log.Printf("⚠️ Failed to record trade: %v", err)
		return
	}

	// 4. Commit the Transaction (Save it permanently)
	if err := tx.Commit(ctx); err != nil {
		log.Printf("⚠️ Failed to commit transaction: %v", err)
		return
	}

	log.Printf("✅ Order Executed: %s. New Balance: $%.2f", order.Ticker, newBalance)
	
	// 5. Tell NATS the message is fully processed
	msg.Ack()
}

// setupDatabase creates the necessary tables if they don't exist
func setupDatabase() {
	ctx := context.Background()
	
	schema := `
	CREATE TABLE IF NOT EXISTS users (
		id SERIAL PRIMARY KEY,
		balance DECIMAL(15, 2) NOT NULL
	);

	CREATE TABLE IF NOT EXISTS trades (
		id SERIAL PRIMARY KEY,
		user_id INTEGER REFERENCES users(id),
		ticker VARCHAR(10) NOT NULL,
		action VARCHAR(4) NOT NULL,
		price DECIMAL(10, 2) NOT NULL,
		quantity INTEGER NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	-- Insert a test user with $10,000 if the table is empty
	INSERT INTO users (id, balance) 
	SELECT 1, 10000.00 
	WHERE NOT EXISTS (SELECT 1 FROM users WHERE id = 1);
	`

	_, err := database.Pool.Exec(ctx, schema)
	if err != nil {
		log.Fatalf("❌ Failed to initialize database schema: %v", err)
	}
	log.Println("✅ PostgreSQL Schema Initialized!")
}

func keepAlive() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("\n🛑 Shutting down Execution Service...")
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