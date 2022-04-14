package manager

import (
	"github.com/spf13/cobra"
	"github.com/stream-stack/dispatcher/pkg/config"
	"time"
)

var address string
var streamName string
var selector string
var connectionRetryDuration time.Duration

func InitFlags() {
	config.RegisterFlags(func(command *cobra.Command) {
		command.PersistentFlags().StringVar(&address, "Manager-Address", "0.0.0.0:8080", "manager address")
		command.PersistentFlags().StringVar(&streamName, "STREAM_NAME", "", "stream name")
		command.PersistentFlags().StringVar(&selector, "SELECTOR", "", "storeset selector")
		command.PersistentFlags().DurationVar(&connectionRetryDuration, "ConnectionRetryDuration", time.Second, "connection retry duration")
	})
}
