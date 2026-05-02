package database

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool is a global variable that holds our active database connections
var Pool *pgxpool.Pool

// Connect initializes the database connection pool
func Connect() error {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return fmt.Errorf("DATABASE_URL environment variable is not set")
	}

	// Create the connection pool (crucial for high-performance backends)
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}

	Pool = pool
	fmt.Println("✅ Successfully connected to PostgreSQL!")
	return nil
}