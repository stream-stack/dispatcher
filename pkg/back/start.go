package back

import (
	"context"
	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/stream-stack/dispatcher/pkg/manager"
	_ "google.golang.org/grpc/health"
)

func StartManager(ctx context.Context) error {
	if err := manager.StartManagerGrpc(ctx); err != nil {
		return err
	}
	go StartHttpServer(ctx)
	go StartPartitionAdder(ctx)
	return nil
}
