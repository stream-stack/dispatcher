package router

import (
	"context"
	"github.com/ryszard/goskiplist/skiplist"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/common/protocol/operator"
)

var PartitionOpCh = make(chan func(ctx context.Context, partitions *skiplist.SkipList), 1)

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

func AddPartition(partition *operator.Partition, post func(partition *operator.Partition, total uint64)) {
	PartitionOpCh <- func(ctx context.Context, partitions *skiplist.SkipList) {
		logrus.Debugf("add partition %+v", partition)
		key := int(partition.Begin)
		if key == 0 {
			key = 1
		}
		partitions.Set(key, partition.Store)
		post(partition, uint64(partitions.Len()))
		logrus.Debugf("add partition end , current partition length: %v", partitions.Len())
	}
}
