package store

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"google.golang.org/grpc"
)

func SaveCloudEvent(ctx context.Context, event *v1.CloudEvent, clientAddr []string) []error {
	clients := make([]*grpc.ClientConn, len(clientAddr))
	for i, s := range clientAddr {
		conn, ok := Clients[s]
		if !ok {
			logrus.Errorf("[store-client]store connection not found for %s", s)
			return []error{fmt.Errorf("store connection not found for %s", s)}
		}
		clients[i] = conn
	}
	total := len(clients)
	c := make(chan interface{}, total)
	for _, client := range clients {
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
