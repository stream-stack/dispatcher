package store

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stream-stack/common/partition"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health"
	"time"
)

var ps = make([]*partition.Set, 0)
var consistent = NewConsistent()

func StartStoreConn(ctx context.Context) error {
	partitionFile := viper.GetString("StorePartitionConfigFile")
	if partitionFile == "" {
		return fmt.Errorf("[store-client]store-partition-config-file not found")
	}
	if t, err := partition.Read(partitionFile); err != nil {
		return err
	} else {
		ps = t
	}

	if err := connectionStore(ctx); err != nil {
		return err
	}

	return initConsistent()
}

func initConsistent() error {
	for _, set := range ps {
		if err := consistent.Add(set); err != nil {
			return err
		}
	}
	return nil
}

func connectionStore(ctx context.Context) error {
	for _, set := range ps {
		set.StoreConn = make([]*grpc.ClientConn, len(set.Addrs))
		for i, addr := range set.Addrs {
			logrus.Debugf("[store-client]begin connection to store address:%v", addr)
			//TODO: 接入grpc中间件,重试,opentelemetry
			client, err := grpc.Dial(addr,
				grpc.WithTimeout(time.Second*2),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				//grpc.WithBlock(),
				grpc.WithDefaultServiceConfig(`{"HealthCheckConfig": {"ServiceName": "store"}}`),
			)
			if err != nil {
				return err
			}
			set.StoreConn[i] = client
		}
	}
	go func() {
		select {
		case <-ctx.Done():
			for _, set := range ps {
				for _, conn := range set.StoreConn {
					if err := conn.Close(); err != nil {
						logrus.Errorf("[store-client]close store connection error:%v", err)
					}
				}
			}
		}
	}()
	return nil
}
