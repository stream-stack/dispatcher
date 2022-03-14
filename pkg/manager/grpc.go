package manager

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
)

func StartGrpc(ctx context.Context) error {

	_, port, err := net.SplitHostPort("")
	if err != nil {
		return fmt.Errorf("failed to parse local address (%q): %v", "", err)
	}
	sock, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	//raft.RaftManager.Register(s)
	//leaderhealth.Setup(raft.Raft, s, []string{"store"})
	//raftadmin.Register(s, raft.Raft)
	reflection.Register(s)
	//protocol.RegisterEventServiceServer(s, NewEventService())
	//protocol.RegisterKVServiceServer(s, NewKVService())
	go func() {
		select {
		case <-ctx.Done():
			s.GracefulStop()
		}
	}()
	if err := s.Serve(sock); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}
