package manager

import (
	"context"
	"fmt"
	"github.com/braintree/manners"
	"github.com/gin-gonic/gin"
	"github.com/stream-stack/dispatcher/pkg/manager/protocol"
	"net/http"
	"time"
)

func StartHttpServer(ctx context.Context) {
	r := gin.Default()
	//health
	//configuration
	//configuration/stores
	//configuration/partitions
	//configuration/maxEventId
	r.Handle(http.MethodGet, "/health", func(c *gin.Context) {
		c.String(http.StatusOK, "healthy")
	})
	r.Handle(http.MethodGet, "/conns", func(c *gin.Context) {
		c.JSON(http.StatusOK, connections)
	})
	r.Handle(http.MethodGet, "/configuration", func(c *gin.Context) {
		c.JSON(http.StatusOK, configuration)
	})
	r.Handle(http.MethodPost, "/configuration/stores", func(c *gin.Context) {
		var stores []protocol.Store
		err := c.BindJSON(&stores)
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
				Store:      store,
			}
			if runner.Connect() != nil {
				c.JSON(http.StatusInternalServerError, err)
				return
			}
			configuration.Stores = append(configuration.Stores, store)
			connections[store.Name] = runner
			runner.Start(func() {
				name := store.Name
				delete(connections, name)
			})
		}
		time.Sleep(time.Second * 4)
		c.JSON(http.StatusOK, configuration)
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
		fmt.Printf("failed start manager serve: %v", err)
	}
}
