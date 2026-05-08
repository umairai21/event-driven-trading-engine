package main

import (
	"context"
	"log"
	"net"

	"github.com/umairai21/event-driven-trading-engine/internal/pb"
	"google.golang.org/grpc"
)

type server struct {
	// UnimplementedRiskServiceServer is required by gRPC for forward compatibility
}


func (s *server) CheckAccount(ctx context.Context, req *pb.RiskRequest) (*pb.RiskResponse, error) {
	log.Printf("📞 gRPC INCOMING: Checking risk for User %d (Trade Cost: $%.2f)", req.UserId, req.TotalCost)


	// Rule 1: We flag any single trade over $5,000 as too risky
	if req.TotalCost > 5000.00 {
		log.Println("❌ gRPC OUTGOING: Trade Rejected (Exceeds single-trade limit)")
		return &pb.RiskResponse{
			IsApproved: false,
			Reason:     "Trade exceeds $5,000 risk limit",
		}, nil
	}

	// Rule 2: Approve everything else
	log.Println("✅ gRPC OUTGOING: Trade Approved")
	return &pb.RiskResponse{
		IsApproved: true,
		Reason:     "Risk check passed",
	}, nil
}

func main() {
	// 1. Open a pure TCP port (gRPC doesn't use HTTP!)
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("❌ Failed to listen on port 50051: %v", err)
	}

	// 2. Create the gRPC Server
	grpcServer := grpc.NewServer()

	// 3. Register our Risk logic to the server
	log.Println("🛡️  Risk Service is running on gRPC port 50051...")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("❌ Failed to serve gRPC: %v", err)
	}
}