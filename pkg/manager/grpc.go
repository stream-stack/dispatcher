package manager

import (
	"context"
	"fmt"
	"github.com/stream-stack/dispatcher/pkg/manager/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
)

type xdsServer struct {
	ctx context.Context
}

func (x *xdsServer) StoreSetPush(ctx context.Context, request *protocol.StoreSetPushRequest) (*protocol.StoreSetPushResponse, error) {
	for _, store := range request.Stores {
		StoreSetAddCh <- store
	}
	return &protocol.StoreSetPushResponse{}, nil
}

func (x *xdsServer) SubscriberPush(ctx context.Context, request *protocol.SubscriberPushRequest) (*protocol.SubscriberPushResponse, error) {
	//dispatcher不会调研到这个接口
	return &protocol.SubscriberPushResponse{}, nil
}

func (x *xdsServer) AllocatePartition(request *protocol.AllocatePartitionRequest, server protocol.XdsService_AllocatePartitionServer) error {
	panic("implement me")
}

func StartManagerGrpc(ctx context.Context) error {
	sock, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	reflection.Register(s)
	protocol.RegisterXdsServiceServer(s, &xdsServer{ctx: ctx})
	go func() {
		select {
		case <-ctx.Done():
			s.GracefulStop()
		}
	}()
	go func() {
		if err := s.Serve(sock); err != nil {
			panic(err)
		}
	}()
	return nil
}
