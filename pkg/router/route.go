package router

import (
	"context"
	"github.com/ryszard/goskiplist/skiplist"
	"github.com/stream-stack/dispatcher/pkg/protocol"
)

var PartitionOpCh = make(chan func(ctx context.Context, partitions *skiplist.SkipList))

var partitionList = skiplist.NewIntMap()

func StartRoute(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case op := <-PartitionOpCh:
			op(ctx, partitionList)
		}
	}
}

func AddPartition(partition *protocol.Partition) {
	PartitionOpCh <- func(ctx context.Context, partitions *skiplist.SkipList) {
		partitions.Set(partition.Begin, partition.Store)
	}
}
