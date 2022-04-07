package manager

import (
	"bytes"
	"context"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/golang/protobuf/proto"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/dispatcher/pkg/protocol"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
)

func TestXdsServer_StoreSetPush(t *testing.T) {
	ctx, cancelFunc := context.WithCancel(context.TODO())
	defer cancelFunc()
	logrus.SetLevel(logrus.TraceLevel)
	dial, err := grpc.Dial("127.0.0.1:8080", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	client := protocol.NewXdsServiceClient(dial)
	sets := make([]*protocol.StoreSet, 1)
	sets[0] = &protocol.StoreSet{
		Name:      "t",
		Namespace: "ns",
		Uris:      []string{`127.0.0.1:2001`, `127.0.0.1:2002`, `127.0.0.1:2003`},
	}
	push, err := client.StoreSetPush(ctx, &protocol.StoreSetPushRequest{
		Stores: sets,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("StoreSetPush 返回值:%+v\n", push)

	serviceConfig := `{"healthCheckConfig": {"serviceName": "store"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	conn, err := grpc.Dial(`multi:///127.0.0.1:2001,127.0.0.1:2002,127.0.0.1:2003`,
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	partition := &protocol.Partition{
		Begin: 1,
		Store: sets[0],
	}
	marshal, err := proto.Marshal(partition)
	if err != nil {
		panic(err)
	}
	eventCli := protocol.NewEventServiceClient(conn)
	apply, err := eventCli.Apply(context.TODO(), &protocol.ApplyRequest{
		StreamName: "_system_broker_partition",
		StreamId:   "t-ns",
		EventId:    1,
		Data:       marshal,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Apply 返回值:%+v\n", apply)

	time.Sleep(time.Second)

	fmt.Println("发送cloudevent")
	request, err := http.NewRequest("POST", "http://127.0.0.1", bytes.NewBuffer([]byte(`test`)))
	if err != nil {
		panic(err)
	}
	request.Header.Set("Ce-Id", "100")
	request.Header.Set("Ce-Specversion", "1.0")
	request.Header.Set("Ce-Subject", "testSubject")
	request.Header.Set("Ce-Type", "test")
	request.Header.Set("Ce-Source", "testSource")
	request.Header.Set("Content-Type", "application/json")
	c := http.Client{}
	do, err := c.Do(request)
	if err != nil {
		panic(err)
	}
	defer do.Body.Close()
	i, err := ioutil.ReadAll(do.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(i), do.Status)
}
