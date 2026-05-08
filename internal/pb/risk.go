package pb

import (
	"context"
	"google.golang.org/grpc"
)

// Request
type RiskRequest struct {
	UserId    int32
	TotalCost float64
}

// Response
type RiskResponse struct {
	IsApproved bool
	Reason     string
}

// The Server Interface
type RiskServiceServer interface {
	CheckAccount(context.Context, *RiskRequest) (*RiskResponse, error)
}

// The Client Interface
type RiskServiceClient interface {
	CheckAccount(ctx context.Context, in *RiskRequest, opts ...grpc.CallOption) (*RiskResponse, error)
}