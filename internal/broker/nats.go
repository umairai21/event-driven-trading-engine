package broker

import (
	"fmt"
	"os"

	"github.com/nats-io/nats.go"
)

// We expose the connection and JetStream context globally for our services to use
var (
	NC *nats.Conn
	JS nats.JetStreamContext
)

// Connect initializes the NATS connection and JetStream
func Connect() error {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL // Fallback if the .env isn't loaded properly
	}

	// 1. Connect to the core NATS server
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// 2. Initialize JetStream (the persistent, event-driven engine)
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to initialize JetStream: %w", err)
	}

	NC = nc
	JS = js
	fmt.Println("✅ Successfully connected to NATS JetStream!")
	return nil
}	