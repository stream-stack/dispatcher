package manager

import (
	"context"
	"encoding/json"
	_ "github.com/Jille/grpc-multi-resolver"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/dispatcher/pkg/manager/protocol"
	"github.com/stream-stack/dispatcher/pkg/router"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health"
	"os"
	"strings"
	"time"
)

type SubscribeRunner struct {
	client protocol.EventServiceClient
	Store  *protocol.StoreSet `json:"store"`
}

func (r *SubscribeRunner) Start(ctx context.Context) {
	hostname, _ := os.Hostname()
	StreamId := os.Getenv("STREAM_NAME")
	logrus.Infof("启动对store的分片stream订阅,hostname:%s,stramid:%s", hostname, StreamId)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_ = r.subscribe(ctx, hostname, StreamId)
			}
		}
	}()
}

func (r *SubscribeRunner) subscribe(ctx context.Context, hostname string, StreamId string) error {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "store"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	join := strings.Join(r.Store.Uris, ",")
	conn, err := grpc.Dial("multi:///"+join,
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	if err != nil {
		logrus.Errorf("连接storset:%s出现错误,%v", join, err)
		return err
	}
	defer conn.Close()
	r.client = protocol.NewEventServiceClient(conn)

	subscribe, err := r.client.Subscribe(ctx, &protocol.SubscribeRequest{
		SubscribeId: hostname,
		Regexp:      "streamName == '_system_broker_partition' && streamId == '" + StreamId + "'",
		Offset:      0,
	})
	if err != nil {
		logrus.Warnf("订阅partition出错,%v", err)
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			recv, err := subscribe.Recv()
			if err != nil {
				logrus.Warnf("接收partition出错,%v", err)
				continue
			}
			logrus.Warnf("接收到分片数据,数据为:%+v", string(recv.Data))
			partition := &protocol.Partition{}
			err = json.Unmarshal(recv.Data, partition)
			if err != nil {
				logrus.Warnf("反序列化partition出错,%v", err)
				continue
			}
			router.PartitionAddCh <- partition

			logrus.Debugf("收到分片消息,%+v", partition)
		}
	}
}
