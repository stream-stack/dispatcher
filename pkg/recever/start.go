package recever

import (
	"context"
	"fmt"
	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"
	http2 "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/ryszard/goskiplist/skiplist"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/dispatcher/pkg/manager"
	protocol2 "github.com/stream-stack/dispatcher/pkg/protocol"
	"github.com/stream-stack/dispatcher/pkg/router"
	"google.golang.org/grpc"
	"net"
	"net/http"
	"strconv"
)

func StartReceive(ctx context.Context) error {
	//TODO:clientHTTP server参数设置
	listen, err := net.Listen("tcp", address)
	if err != nil {
		logrus.Errorf("[recever]listen %s error:%s", address, err)
		return err
	}
	clientHTTP, err := v2.NewClientHTTP(http2.WithListener(listen))
	if err != nil {
		return err
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = listen.Close()
		}
	}()
	err = clientHTTP.StartReceiver(ctx, handler)
	if err != nil {
		return err
	}
	return nil
}

func handler(ctx context.Context, event event.Event) protocol.Result {
	json, err := event.MarshalJSON()
	if err != nil {
		return err
	}
	parseUint, err := strconv.ParseUint(event.ID(), 10, 64)
	if err != nil {
		return err
	}
	result := make(chan error, 1)
	router.PartitionOpCh <- func(ctx context.Context, partitions *skiplist.SkipList) {
		iterator := partitions.Range(0, parseUint)
		iterator.Previous()
		var store *protocol2.StoreSet
		for iterator.Next() {
			store = iterator.Value().(*protocol2.StoreSet)
		}
		if store == nil {
			result <- http2.NewResult(http.StatusNotFound, "partition not found for %s", event.ID())
			return
		}

		manager.StoreSetConnOperation <- func(m map[string]*manager.StoreSetConn) {
			conn := manager.GetOrCreateConn(ctx, m, store)
			conn.OpCh <- func(ctx context.Context, connection *grpc.ClientConn, Store *protocol2.StoreSet) {
				client := protocol2.NewEventServiceClient(connection)
				apply, err := client.Apply(ctx, &protocol2.ApplyRequest{
					StreamName: event.Subject(),
					StreamId:   event.Source(),
					EventId:    parseUint,
					Data:       json,
				})
				if err != nil {
					result <- err
					return
				}
				if !apply.Ack {
					result <- fmt.Errorf(apply.Message)
				}
				result <- protocol.ResultACK
			}
		}

	}
	return <-result
}
