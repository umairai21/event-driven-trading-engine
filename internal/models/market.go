package models

import "time"


type PriceTick struct {
	Ticker    string    `json:"ticker"`
	Price     float64   `json:"price"`
	Timestamp time.Time `json:"timestamp"`
}


type OrderRequest struct {
	Ticker    string    `json:"ticker"`
	Action    string    `json:"action"` 
	Price     float64   `json:"price"`
	Quantity  int       `json:"quantity"`
	Timestamp time.Time `json:"timestamp"`
}