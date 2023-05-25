package store

import (
	"github.com/spf13/cobra"
	"github.com/stream-stack/dispatcher/pkg/config"
	"time"
)

func InitFlags() {
	config.RegisterFlags(func(c *cobra.Command) {
		c.PersistentFlags().StringSlice("store-address-list",
			[]string{"localhost:8080", "localhost:8081", "localhost:8082"}, "store server address list")
		c.PersistentFlags().Duration("store-timeout", time.Second*2, "store cloudevent timeout")
	})
}
