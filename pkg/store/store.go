package store

import (
	"context"
	"fmt"
	"github.com/spf13/viper"
	"github.com/stream-stack/common"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"google.golang.org/grpc"
	"strconv"
)

func formatPartitionKey(event *v1.CloudEvent) []byte {
	return []byte(fmt.Sprintf("%s/%s", event.GetSource(), event.GetId()))
}

func SaveCloudEvent(ctx context.Context, event *v1.CloudEvent) []error {
	key := formatPartitionKey(event)
	set, slot := consistent.GetNode(key)
	//add slot to event
	event.Attributes[common.CloudEventAttrSlotKey] = &v1.CloudEvent_CloudEventAttributeValue{
		Attr: &v1.CloudEvent_CloudEventAttributeValue_CeString{CeString: strconv.FormatUint(uint64(slot), 10)},
	}
	total := len(set.storeConn)
	c := make(chan interface{}, total)
	for _, client := range set.storeConn {
		go sendCloudEvent(ctx, client, event, c)
	}
	errCount := 0
	errs := make([]error, 0)
	for i := 0; i < total; i++ {
		select {
		case <-ctx.Done():
			return []error{ctx.Err()}
		case result := <-c:
			err, ok := result.(error)
			if ok {
				errCount++
				errs = append(errs, err)
			}
			if errCount > total/2 {
				return errs
			}
		}
	}
	return []error{}
}

func sendCloudEvent(ctx context.Context, client *grpc.ClientConn, event *v1.CloudEvent, c chan interface{}) {
	storeClient := v1.NewStoreClient(client)
	duration := viper.GetDuration(`store-timeout`)
	timeout, cancelFunc := context.WithTimeout(ctx, duration)
	defer cancelFunc()
	store, err := storeClient.Store(timeout, event)
	if err != nil {
		c <- err
		return
	}
	c <- store
}
