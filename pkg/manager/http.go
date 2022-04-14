package manager

import (
	"context"
	"fmt"
	"github.com/braintree/manners"
	"github.com/gin-gonic/gin"
	"net/http"
)

func StartManagerHttp(ctx context.Context) error {
	engine := gin.Default()
	engine.GET("/health", func(c *gin.Context) {
		c.String(http.StatusOK, "healthy")
	})
	go func() {
		select {
		case <-ctx.Done():
			manners.Close()
		}
	}()
	go func() {
		if err := manners.ListenAndServe(address, engine); err != nil {
			fmt.Printf("failed start manager serve: %v", err)
		}
	}()
	return nil
}
