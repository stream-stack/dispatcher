package manager

import (
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/braintree/manners"
	"github.com/gin-gonic/gin"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/stream-stack/dispatcher/pkg/manager/protocol"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health"
	"net/http"
	"os"
	"strings"
	"time"
)

var configuration = &protocol.Configuration{}

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
	go func() {
		subscribe, err := r.client.Subscribe(r.ctx, &protocol.SubscribeRequest{
			SubscribeId: hostname,
			Regexp:      "streamName='_system_broker_partition'&& streamId =~ ''",
			Offset:      0,
		})
		if err != nil {
			//TODO:错误处理
		}
		for {
			select {
			case <-r.ctx.Done():
				return
			default:
				recv, err := subscribe.Recv()
				if err != nil {
					//					panic(err)
				}
				//TODO:将partition加入配置中
				fmt.Println(recv)
			}
		}
	}()
}

var connections = make(map[string]*SubscribeRunner)

func StartManager(ctx context.Context) error {
	r := gin.Default()
	//configuration
	//configuration/stores
	//configuration/partitions
	//configuration/maxEventId
	r.Handle(http.MethodGet, "/configuration", func(c *gin.Context) {
		c.JSON(http.StatusOK, configuration)
	})
	r.Handle(http.MethodPost, "/configuration/stores", func(c *gin.Context) {
		data, err := c.GetRawData()
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}
		var stores []protocol.Store
		err = json.Unmarshal(data, &stores)
		if err != nil {
			c.JSON(http.StatusInternalServerError, err)
			return
		}
		for _, store := range stores {
			_, ok := connections[store.Name]
			if ok {
				continue
			}
			cancel, cancelFunc := context.WithCancel(ctx)
			runner := &SubscribeRunner{
				ctx:        cancel,
				cancelFunc: cancelFunc,
				store:      store,
			}
			if runner.Connect() != nil {
				c.JSON(http.StatusInternalServerError, err)
				return
			}
			configuration.Stores = append(configuration.Stores, store)
			connections[store.Name] = runner
			runner.Start()
		}
		time.Sleep(time.Second * 4)
		c.JSON(http.StatusOK, configuration)
		//configuration.Stores = stores
	})
	//r.Handle(http.MethodGet, "/configuration/partitions", func(c *gin.Context) {
	//	c.JSON(http.StatusOK, configuration.Partitions)
	//})
	//r.Handle(http.MethodGet, "/configuration/maxEventId", func(c *gin.Context) {
	//	c.JSON(http.StatusOK, configuration.MaxEventId)
	//})

	go func() {
		select {
		case <-ctx.Done():
			manners.Close()
		}
	}()
	if err := manners.ListenAndServe(address, r); err != nil {
		return fmt.Errorf("failed start manager serve: %v", err)
	}
	return nil
}
