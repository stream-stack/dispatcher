package manager

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/cloudevents/sdk-go/v2/event"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
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
	store      protocol.Store
	cancelFunc context.CancelFunc
}

func (r *SubscribeRunner) Connect() error {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "store"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	conn, err := grpc.Dial("dns:///"+strings.Join(r.store.Uris, ","),
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	if err != nil {
		return err
	}
	r.client = protocol.NewEventServiceClient(conn)
	return nil
}

func (r *SubscribeRunner) Start() {
	hostname, _ := os.Hostname()
	StreamId := os.Getenv("STREAM_ID")
	go func() {
		subscribe, err := r.client.Subscribe(r.ctx, &protocol.SubscribeRequest{
			SubscribeId: hostname,
			Regexp:      "streamName='_system_broker_partition' && streamId = '" + StreamId + "'",
			Offset:      0,
		})
		if err != nil {
			//TODO:错误处理
			panic(err)
		}
		for {
			select {
			case <-r.ctx.Done():
				return
			default:
				recv, err := subscribe.Recv()
				if err != nil {
					panic(err)
				}
				partition := &protocol.Partition{}
				err = binary.Read(bytes.NewBuffer(recv.Data), binary.BigEndian, partition)
				if err != nil {
					panic(err)
				}
				partitionAddCh <- *partition

				fmt.Println(recv)
			}
		}
	}()
}

var connections = make(map[string]*SubscribeRunner)

func GetStoreAddr(e event.Event) protocol.EventServiceClient {
	//TODO:根据e和已存在的分片,返回store的地址
	for _, runner := range connections {
		return runner.client
	}
	return nil
}
