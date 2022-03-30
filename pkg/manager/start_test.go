package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/stream-stack/dispatcher/pkg/manager/protocol"
	"google.golang.org/grpc"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
)

func TestStartManager(t *testing.T) {
	//StartManager(context.TODO())
	//
	//time.Sleep(time.Second * 15)

	store := protocol.Store{
		Name:      "t",
		Namespace: "tN",
		Uris:      []string{`127.0.0.1:2001`, `127.0.0.1:2002`, `127.0.0.1:2003`},
	}
	postData, _ := json.Marshal([]protocol.Store{store})
	post, err := http.Post("http://127.0.0.1:8080/configuration/stores", `application/json`, bytes.NewBuffer(postData))
	if err != nil {
		panic(err)
	}
	defer post.Body.Close()
	all, err := ioutil.ReadAll(post.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(all))

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
	partitionData, err := json.Marshal(protocol.Partition{
		RangeRegexp: "[0-9]{1,5}",
		Store:       store,
	})
	eventCli := protocol.NewEventServiceClient(conn)
	apply, err := eventCli.Apply(context.TODO(), &protocol.ApplyRequest{
		StreamName: "_system_broker_partition",
		StreamId:   "t-tN",
		EventId:    "1",
		Data:       partitionData,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(apply)

	time.Sleep(time.Second * 5)

	get, err := http.Get("http://127.0.0.1:8080/configuration")
	if err != nil {
		panic(err)
	}
	defer get.Body.Close()
	readAll, err := ioutil.ReadAll(get.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(readAll))

	cancel, cancelFunc := context.WithCancel(context.TODO())
	defer cancelFunc()
	go func() {
		subscribe, err := eventCli.Subscribe(cancel, &protocol.SubscribeRequest{
			SubscribeId: "test",
			Regexp:      "streamName== 'testSubject' && streamId== 'testSource'",
			//Regexp: "streamName=~ '[a-z]+'",
			Offset: 0,
		})
		if err != nil {
			panic(err)
		}
		for {
			select {
			case <-cancel.Done():
				return
			default:
				recv, err := subscribe.Recv()
				if err != nil {
					panic(err)
				}
				fmt.Println("接收cloudevent")
				fmt.Printf("%+v \n", recv)
			}

		}
	}()

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
	client := http.Client{}
	do, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	defer do.Body.Close()
	i, err := ioutil.ReadAll(do.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(i), do.Status)

	time.Sleep(time.Second * 100)

}
