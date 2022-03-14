package recever

import (
	"context"
	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"
	http2 "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/sirupsen/logrus"
	"net"
)

func StartReceive(ctx context.Context) error {
	//TODO:http server参数设置
	listen, err := net.Listen("TCP", address)
	if err != nil {
		logrus.Errorf("[recever]listen %s error:%s", address, err)
		return err
	}
	http, err := v2.NewClientHTTP(http2.WithListener(listen))
	if err != nil {
		return err
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = listen.Close()
		}
	}()
	err = http.StartReceiver(ctx, handler)
	if err != nil {
		return err
	}
	return nil
}

func handler(context.Context, event.Event) protocol.Result {
	//根据event获取分片
	//根据分片对应的storeset,发送消息
	return nil
}
