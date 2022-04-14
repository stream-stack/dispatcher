package manager

import (
	"context"
	pp "github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/dispatcher/pkg/protocol"
	"github.com/stream-stack/dispatcher/pkg/router"
	"google.golang.org/grpc"
	"os"
	"strings"
)

func subscribePartition(ctx context.Context, conn *grpc.ClientConn, Store *protocol.StoreSet) {
	hostname, _ := os.Hostname()
	logrus.Infof("start partition subscribe for storeset %s,hostname:%s,stramid:%s", strings.Join(Store.Uris, ","), hostname, streamName)
	client := protocol.NewEventServiceClient(conn)
	subscribe, err := client.Subscribe(ctx, &protocol.SubscribeRequest{
		SubscribeId: hostname,
		Regexp:      "streamName == '_system_broker_partition' && streamId == '" + streamName + "'",
		Offset:      0,
	})
	if err != nil {
		logrus.Errorf("subscribe error,%v", err)
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				recv, err := subscribe.Recv()
				if err != nil {
					logrus.Errorf("recv partition error,%v", err)
					continue
				}
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
