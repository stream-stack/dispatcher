package manager

import (
	"context"
	pp "github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/dispatcher/pkg/protocol"
	"github.com/stream-stack/dispatcher/pkg/router"
	"google.golang.org/grpc"
	"os"
)

func subscribePartition(ctx context.Context, conn *grpc.ClientConn, Store *protocol.StoreSet) {
	hostname, _ := os.Hostname()
	StreamId := os.Getenv("STREAM_NAME")
	logrus.Infof("启动对store的分片stream订阅,hostname:%s,stramid:%s", hostname, StreamId)
	client := protocol.NewEventServiceClient(conn)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				subscribe, err := client.Subscribe(ctx, &protocol.SubscribeRequest{
					SubscribeId: hostname,
					Regexp:      "streamName == '_system_broker_partition' && streamId == '" + StreamId + "'",
					Offset:      0,
				})
				if err != nil {
					logrus.Errorf("订阅partition出错,%v", err)
					continue
				}

				recv, err := subscribe.Recv()
				if err != nil {
					logrus.Errorf("接收partition出错,%v", err)
					continue
				}
				logrus.Debugf("接收到分片数据,数据为:%+v", string(recv.Data))
				partition := &protocol.Partition{}
				if err = pp.Unmarshal(recv.Data, partition); err != nil {
					logrus.Errorf("反序列化分片数据出错,%v", err)
				}
				logrus.Debugf("收到分片消息,%+v", partition)
				router.AddPartition(partition)
			}
		}
	}()
}
