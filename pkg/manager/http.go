package manager

import (
	"context"
	"fmt"
	"github.com/braintree/manners"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/gin-gonic/gin"
	"github.com/stream-stack/dispatcher/pkg/protocol"
	"net/http"
)

var statisticsInst = &statistics{}

type statistics struct {
	Uris              []string `json:"uris,omitempty"`
	PartitionCount    uint64   `json:"partitionCount,omitempty"`
	MaxPartitionBegin uint64   `json:"maxPartitionBegin,omitempty"`
	MaxEvent          uint64   `json:"maxEvent,omitempty"`
	TotalDataSize     uint64   `json:"totalDataSize,omitempty"`
}

func SetStatisticsWithPartition(partition *protocol.Partition, total uint64) {
	if statisticsInst.MaxPartitionBegin <= partition.Begin {
		statisticsInst.MaxPartitionBegin = partition.Begin
		statisticsInst.Uris = partition.Store.Uris
	}
	statisticsInst.PartitionCount = total
}

func SetStatisticsWithCloudEvent(e event.Event, eid uint64, b []byte) {
	if statisticsInst.MaxEvent <= eid {
		statisticsInst.MaxEvent = eid
	}
	statisticsInst.TotalDataSize += uint64(len(b))
}

func StartManagerHttp(ctx context.Context, cancelFunc context.CancelFunc) error {
	engine := gin.Default()
	engine.GET("/health", func(c *gin.Context) {
		c.String(http.StatusOK, "healthy")
	})
	engine.GET("/statistics", func(c *gin.Context) {
		c.JSON(http.StatusOK, statisticsInst)
	})
	go func() {
		select {
		case <-ctx.Done():
			manners.Close()
		}
	}()
	go func() {
		if err := manners.ListenAndServe(address, engine); err != nil {
			fmt.Printf("failed start manager serve: %v", err)
			cancelFunc()
		}
	}()
	return nil
}
