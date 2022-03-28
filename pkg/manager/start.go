package manager

import (
	"context"
	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/stream-stack/dispatcher/pkg/manager/protocol"
	_ "google.golang.org/grpc/health"
)

var configuration = &protocol.Configuration{}

func StartManager(ctx context.Context) error {
	go StartHttpServer(ctx)
	go StartPartitionAdder(ctx)
	return nil
}
