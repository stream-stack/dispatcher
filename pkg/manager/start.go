package manager

import (
	"context"
	"fmt"
	"github.com/braintree/manners"
	"github.com/gin-gonic/gin"
)

func StartManager(ctx context.Context) error {
	r := gin.Default()
	//接收 storeset
	//接收 partition信息
	//统计 分片情况

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
