package models

import "time"

// PriceTick represents a single price update for a stock.
// The `json:"..."` tags tell Go how to format this when sending it over the network.
type PriceTick struct {
	Ticker    string    `json:"ticker"`
	Price     float64   `json:"price"`
	Timestamp time.Time `json:"timestamp"`
}

// OrderRequest represents a decision made by the Strategy Engine to buy or sell.
type OrderRequest struct {
	Ticker    string    `json:"ticker"`
	Action    string    `json:"action"` // "BUY" or "SELL"
	Price     float64   `json:"price"`
	Quantity  int       `json:"quantity"`
	Timestamp time.Time `json:"timestamp"`
}