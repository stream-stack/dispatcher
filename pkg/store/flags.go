package store

import (
	"github.com/spf13/cobra"
	"github.com/stream-stack/dispatcher/pkg/config"
	"time"
)

func InitFlags() {
	config.RegisterFlags(func(c *cobra.Command) {
		c.PersistentFlags().Duration("StoreTimeout", time.Second*2, "store cloudevent timeout")
		c.PersistentFlags().String("StorePartitionConfigFile", "./conf/partitions.json", "store partition config file")
	})
}
