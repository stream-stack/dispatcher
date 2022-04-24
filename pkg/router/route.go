package router

import (
	"context"
	"github.com/huandu/skiplist"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/common/protocol/operator"
)

var PartitionOpCh = make(chan func(ctx context.Context, partitions *skiplist.SkipList), 1)

var partitionList = skiplist.New(skiplist.Uint64Desc)

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

func AddPartition(partition *operator.Partition, post func(partition *operator.Partition, total uint64)) {
	PartitionOpCh <- func(ctx context.Context, partitions *skiplist.SkipList) {
		logrus.Debugf("add partition %+v", partition)
		partitions.Set(partition.Begin, partition.Store)
		total := partitions.Len()
		post(partition, uint64(total))
		logrus.Debugf("add partition end , current partition length: %v", total)
	}
}
