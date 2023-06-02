package protocol

import (
	"github.com/spf13/cobra"
	"github.com/stream-stack/dispatcher/pkg/config"
)

func InitFlags() {
	config.RegisterFlags(func(c *cobra.Command) {
		c.PersistentFlags().StringSlice("ProtocolPluginDir", []string{"plugins"}, "protocol-plugin-dir")
	})

}
