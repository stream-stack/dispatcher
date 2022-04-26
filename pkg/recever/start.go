package recever

import (
	"context"
	"fmt"
	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"
	http2 "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/huandu/skiplist"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/common/protocol/operator"
	store2 "github.com/stream-stack/common/protocol/store"
	"github.com/stream-stack/dispatcher/pkg/manager"
	"github.com/stream-stack/dispatcher/pkg/router"
	"google.golang.org/grpc"
	"net"
	"net/http"
	"strconv"
)

func StartReceive(ctx context.Context, cancelFunc context.CancelFunc) error {
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
	go func() {
		err = clientHTTP.StartReceiver(ctx, handler)
		if err != nil {
			logrus.Errorf("start cloudevents receiver error:%v", err)
			cancelFunc()
		}
	}()
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
		find := partitions.Find(parseUint)
		if find == nil {
			result <- http2.NewResult(http.StatusNotFound, "partition not found for %s", event.ID())
			return
		}
		store := find.Value.(*operator.StoreSet)
		logrus.Debugf(`获取到的store为 %+v,eventId:%d`, store, parseUint)
		if store == nil {
			result <- http2.NewResult(http.StatusNotFound, "partition not found for %s", event.ID())
			return
		}

		manager.StoreSetConnOperation <- func(m map[string]*manager.StoreSetConn) {
			conn := manager.GetOrCreateConn(ctx, m, store)
			conn.OpCh <- func(ctx context.Context, connection *grpc.ClientConn, Store *operator.StoreSet) {
				client := store2.NewEventServiceClient(connection)
				logrus.Debugf(`开始向storeset中发送cloudEvent数据`)
				apply, err := client.Apply(ctx, &store2.ApplyRequest{
					StreamName: manager.StreamName,
					StreamId:   event.Source(),
					EventId:    parseUint,
					Data:       json,
				})
				logrus.Debugf(`向storeset中发送cloudEvent数据,返回值为:%+v,error:%v`, apply, err)
				if err != nil {
					result <- err
					return
				}
				if !apply.Ack {
					result <- fmt.Errorf(apply.Message)
					return
				}
				result <- protocol.ResultACK
				manager.SetStatisticsWithCloudEvent(event, parseUint, json)
			}
		}

	}
	return <-result
}
