package store

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

var StoreClients = make(map[string]*grpc.ClientConn)

func StartStoreConn(ctx context.Context) error {
	addrs := viper.GetStringSlice("store-address-list")
	total := len(addrs)
	for i, s := range addrs {
		logrus.Debugf("[store-client][%d/%d]begin connection to store address:%v", i+1, total, s)
		//TODO: 接入grpc中间件,重试,opentelemetry
		client, err := grpc.Dial(s, grpc.WithInsecure())
		if err != nil {
			return err
		}
		StoreClients[s] = client
	}
	go func() {
		select {
		case <-ctx.Done():
			for _, v := range StoreClients {
				_ = v.Close()
			}
		}
	}()
	return nil
}
