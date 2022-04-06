package manager

import (
	"context"
	"fmt"
	_ "github.com/Jille/grpc-multi-resolver"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/dispatcher/pkg/protocol"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/health"
	"strings"
	"time"
)

var StoreSetConnOperation = make(chan func(map[string]*StoreSetConn))

type StoreSetConn struct {
	connection *grpc.ClientConn
	Store      *protocol.StoreSet `json:"store"`

	cancelFunc context.CancelFunc
	OpCh       chan func(ctx context.Context, connection *grpc.ClientConn, Store *protocol.StoreSet)
}

func (c *StoreSetConn) Start(ctx context.Context) {
	go func() {
		cancel, cancelFunc := context.WithCancel(ctx)
		c.cancelFunc = cancelFunc

		for {
			select {
			case <-cancel.Done():
				return
			default:
				err := c.connect()
				if err != nil {
					logrus.Errorf("connect storeset %s error:%v,sleep %v retry", c.Store.Uris, err, connectionRetryDuration)
					time.Sleep(connectionRetryDuration)
					continue
				}
				c.work(cancel)
			}
		}
	}()
}

func GetOrCreateConn(ctx context.Context, m map[string]*StoreSetConn, store *protocol.StoreSet) *StoreSetConn {
	name := GetStoreSetName(store)
	conn, ok := m[name]
	if ok {
		return conn
	}
	conn = &StoreSetConn{Store: store}
	conn.Start(ctx)
	m[name] = conn
	return conn
}

func (c *StoreSetConn) connect() error {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "store"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(5),
	}
	join := strings.Join(c.Store.Uris, ",")
	conn, err := grpc.Dial("multi:///"+join,
		grpc.WithDefaultServiceConfig(serviceConfig), grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)))
	if err != nil {
		logrus.Errorf("连接storset:%s出现错误,%v", join, err)
		return err
	}
	c.connection = conn
	return nil
}

func (c *StoreSetConn) Stop() {
	c.cancelFunc()
	c.connection.Close()
}

func (c *StoreSetConn) work(ctx context.Context) {
	c.OpCh = make(chan func(ctx context.Context, connection *grpc.ClientConn, Store *protocol.StoreSet))
	defer func() {
		close(c.OpCh)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case op := <-c.OpCh:
			op(ctx, c.connection, c.Store)
		}
	}
}

var connMaps = make(map[string]*StoreSetConn)

func StartStoreSetConnManager(ctx context.Context) {
	defer func() {
		for _, conn := range connMaps {
			conn.Stop()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case op := <-StoreSetConnOperation:
			op(connMaps)
		}
	}

}

func GetStoreSetName(add *protocol.StoreSet) string {
	return fmt.Sprintf(`%s/%s`, add.Namespace, add.Name)
}
