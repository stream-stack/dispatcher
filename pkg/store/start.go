package store

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health"
	"os"
	"time"
)

var ps = make([]*PartitionSet, 0)
var consistent = NewConsistent()

func StartStoreConn(ctx context.Context) error {
	partitionFile := viper.GetString("store-partition-config-file")
	if partitionFile == "" {
		return fmt.Errorf("[store-client]store-partition-config-file not found")
	}
	if err := initPartition(partitionFile); err != nil {
		return err
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
		set.storeConn = make([]*grpc.ClientConn, len(set.Addrs))
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
			set.storeConn[i] = client
		}
	}
	go func() {
		select {
		case <-ctx.Done():
			for _, set := range ps {
				for _, conn := range set.storeConn {
					if err := conn.Close(); err != nil {
						logrus.Errorf("[store-client]close store connection error:%v", err)
					}
				}
			}
		}
	}()
	return nil
}

func initPartition(file string) error {
	readFile, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	t := make([]*PartitionSet, 0)
	if err := json.Unmarshal(readFile, &t); err != nil {
		return err
	}
	ps = t
	return nil
}

type PartitionSet struct {
	Name             string   `json:"name"`
	VirtualNodeCount int      `json:"virtualNodeCount"`
	Addrs            []string `json:"addrs"`
	storeConn        []*grpc.ClientConn
}

type PartitionSets []PartitionSet
