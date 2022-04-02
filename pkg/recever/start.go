package recever

import (
	"context"
	"fmt"
	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"
	http2 "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/dispatcher/pkg/back"
	"github.com/stream-stack/dispatcher/pkg/manager"
	protocol2 "github.com/stream-stack/dispatcher/pkg/manager/protocol"
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
	//根据event获取分片
	//根据分片对应的storeset,发送消息
	find, b := back.Find(event.ID())
	if !b {
		return http2.NewResult(http.StatusNotFound, "partition not found for %s", event.ID())
	}
	store := find.(protocol2.Store)
	conn := manager.GetStoreConnection(store)
	if conn == nil {
		return http2.NewResult(http.StatusInternalServerError, "store connection not found for %s", store.Name)
	}
	json, err := event.MarshalJSON()
	if err != nil {
		return err
	}
	parseUint, err := strconv.ParseUint(event.ID(), 10, 64)
	if err != nil {
		return err
	}
	apply, err := conn.Apply(ctx, &protocol2.ApplyRequest{
		StreamName: event.Subject(),
		StreamId:   event.Source(),
		EventId:    parseUint,
		Data:       json,
	})
	if err != nil {
		return err
	}
	if !apply.Ack {
		return fmt.Errorf(apply.Message)
	}
	return protocol.ResultACK
}
