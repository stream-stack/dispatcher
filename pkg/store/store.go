package store

import (
	"context"
	"github.com/spf13/viper"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"github.com/stream-stack/common/util"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strconv"
)

func SaveCloudEvent(ctx context.Context, event *v1.CloudEvent) []error {
	key := util.FormatKeyWithEvent(event)
	set, slot := consistent.GetNode(key)
	request := &v1.CloudEventStoreRequest{
		Event:     event,
		Slot:      strconv.FormatUint(uint64(slot), 10),
		Timestamp: timestamppb.Now(),
	}
	total := len(set.StoreConn)
	c := make(chan interface{}, total)
	for _, client := range set.StoreConn {
		go sendCloudEvent(ctx, client, request, c)
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

func sendCloudEvent(ctx context.Context, client *grpc.ClientConn, request *v1.CloudEventStoreRequest, c chan interface{}) {
	storeClient := v1.NewPublicEventServiceClient(client)
	duration := viper.GetDuration(`StoreTimeout`)
	timeout, cancelFunc := context.WithTimeout(ctx, duration)
	defer cancelFunc()
	store, err := storeClient.Store(timeout, request)
	if err != nil {
		c <- err
		return
	}
	c <- store
}
