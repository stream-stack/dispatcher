package manager

import (
	"context"
	"fmt"
	"github.com/stream-stack/dispatcher/pkg/manager/protocol"
)

var partitionSubscribeRunner = make(map[string]*SubscribeRunner)

var StoreSetAddCh = make(chan *protocol.StoreSet, 1)

func StartPartitionSubscribeManager(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case add := <-StoreSetAddCh:
			name := getStoreSetName(add)
			runner, ok := partitionSubscribeRunner[name]
			if ok {
				continue
			}
			runner = &SubscribeRunner{
				Store: add,
			}
			runner.Start(ctx)
		}
	}
}

func getStoreSetName(add *protocol.StoreSet) string {
	return fmt.Sprintf(`%s/%s`, add.Namespace, add.Name)
}
