package router

import (
	"context"
	"github.com/stream-stack/dispatcher/pkg/manager/protocol"
)

var PartitionAddCh = make(chan *protocol.Partition)
var partitionMap = make(map[uint64]*protocol.Partition)

func StartRoute(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case add := <-PartitionAddCh:
			partitionMap[add.Begin] = add
		}
	}
}
