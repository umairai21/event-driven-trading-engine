package main

import (
	"context"
	"testing"

	"github.com/umairai21/event-driven-trading-engine/internal/pb"
)

func TestCheckAccount(t *testing.T) {
	s := &server{}
	ctx := context.Background()

	tests := []struct {
		name        string
		request     *pb.RiskRequest
		wantApprove bool
	}{
		{
			name:        "Safe Trade under $5000",
			request:     &pb.RiskRequest{UserId: 1, TotalCost: 1000.00},
			wantApprove: true,
		},
		{
			name:        "Risky Trade exactly $5000",
			request:     &pb.RiskRequest{UserId: 1, TotalCost: 5000.00},
			wantApprove: true,
		},
		{
			name:        "Fraud/Risk Trade over $5000",
			request:     &pb.RiskRequest{UserId: 1, TotalCost: 6000.00},
			wantApprove: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := s.CheckAccount(ctx, tt.request)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if response.IsApproved != tt.wantApprove {
				t.Errorf("Expected IsApproved to be %v, but got %v", tt.wantApprove, response.IsApproved)
			}
		})
	}
}