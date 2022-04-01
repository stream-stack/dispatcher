package manager

import (
	"context"
	"encoding/json"
	_ "github.com/Jille/grpc-multi-resolver"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/dispatcher/pkg/manager/protocol"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health"
	"os"
	"strings"
	"time"
)

type SubscribeRunner struct {
	ctx        context.Context
	client     protocol.EventServiceClient
	Store      protocol.Store `json:"store"`
	cancelFunc context.CancelFunc
}

func (r *SubscribeRunner) Connect() error {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "store"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	conn, err := grpc.Dial("multi:///"+strings.Join(r.Store.Uris, ","),
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	if err != nil {
		return err
	}
	r.client = protocol.NewEventServiceClient(conn)
	return nil
}

func (r *SubscribeRunner) Start(clear func()) {
	hostname, _ := os.Hostname()
	StreamId := os.Getenv("STREAM_NAME")
	logrus.Infof("启动对store的分片stream订阅,hostname:%s,stramid:%s", hostname, StreamId)
	go func() {
		defer clear()
		subscribe, err := r.client.Subscribe(r.ctx, &protocol.SubscribeRequest{
			SubscribeId: hostname,
			Regexp:      "streamName == '_system_broker_partition' && streamId == '" + StreamId + "'",
			Offset:      0,
		})
		if err != nil {
			logrus.Warnf("订阅partition出错,%v", err)
		}
		for {
			select {
			case <-r.ctx.Done():
				return
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
				partitionAddCh <- *partition

				logrus.Debugf("收到分片消息,%+v", partition)
			}
		}
	}()
}

var connections = make(map[string]*SubscribeRunner)

func GetStoreConnection(store protocol.Store) protocol.EventServiceClient {
	runner, ok := connections[store.Name]
	if !ok {
		return nil
	}
	return runner.client
}
