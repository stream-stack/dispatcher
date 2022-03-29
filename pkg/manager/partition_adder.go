package manager

import (
	"context"
	"github.com/stream-stack/dispatcher/pkg/manager/protocol"
	"github.com/stream-stack/dispatcher/pkg/router"
)

var partitionAddCh = make(chan protocol.Partition, 1)

func StartPartitionAdder(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case partition := <-partitionAddCh:
			exists := false
			for _, p := range configuration.Partitions {
				if p.RangeRegexp == partition.RangeRegexp {
					exists = true
					continue
				}
			}
			if exists {
				continue
			}
			configuration.Partitions = append(configuration.Partitions, partition)
			_ = router.AddNode(partition.RangeRegexp, partition.Store)
		}
	}
}
