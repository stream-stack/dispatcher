package store

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health"
	"time"
)

var Clients = make(map[string]*grpc.ClientConn)

func StartStoreConn(ctx context.Context) error {
	addrs := viper.GetStringSlice("store-address-list")
	total := len(addrs)
	for i, s := range addrs {
		logrus.Debugf("[store-client][%d/%d]begin connection to store address:%v", i+1, total, s)
		//TODO: 接入grpc中间件,重试,opentelemetry
		client, err := grpc.Dial(s,
			grpc.WithTimeout(time.Second*2),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithDefaultServiceConfig(`{"HealthCheckConfig": {"ServiceName": "store"}}`),
		)
		if err != nil {
			return err
		}
		Clients[s] = client
	}
	go func() {
		select {
		case <-ctx.Done():
			for _, v := range Clients {
				_ = v.Close()
			}
		}
	}()
	return nil
}
