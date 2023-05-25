package main

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"net"
)

//TODO:配置管理

var storeFunc func(ctx context.Context, event *v1.CloudEvent) []error

func StartPlugin(ctx context.Context, f func(ctx context.Context, event *v1.CloudEvent) []error) error {
	logrus.Debugf("[plugin][cloudevents] start cloudevents plugin ...")
	address := ":7080"
	listen, err := net.Listen("tcp", address)
	if err != nil {
		logrus.Errorf("[plugin][cloudevents]listen %s error:%s", address, err)
		return err
	}
	clientHTTP, err := cloudevents.NewClientHTTP(http.WithListener(listen))
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
		err = clientHTTP.StartReceiver(ctx, receiver)
		if err != nil {
			logrus.Errorf("[plugin][cloudevents]start cloudevents receiver error:%v", err)
		}
	}()
	storeFunc = f
	logrus.Debugf("[plugin][cloudevents] start cloudevents plugin finish")
	return nil
}

func receiver(ctx context.Context, e event.Event) protocol.Result {
	logrus.Debugf("[plugin][cloudevents]recv cloudevents:%+v", e)
	var err error
	pe := &v1.CloudEvent{
		Id:          e.ID(),
		Source:      e.Source(),
		SpecVersion: e.SpecVersion(),
		Type:        e.Type(),
		Data:        &v1.CloudEvent_BinaryData{BinaryData: e.Data()},
	}
	//err = protojson.Unmarshal(json, pe)
	//if err != nil {
	//	logrus.Debugf(`[plugin][cloudevents]unmarshal event(json) to protobuf error:%v`, err)
	//	return err
	//}
	errors := storeFunc(ctx, pe)
	if len(errors) > 0 {
		logrus.Errorf("[plugin][cloudevents]store event error:%v", errors)
		return multierror.Append(err, errors...)
	} else {
		logrus.Debugf("[plugin][cloudevents]store event success")
		return nil
	}
}
