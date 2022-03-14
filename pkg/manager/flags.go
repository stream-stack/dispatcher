package manager

import (
	"github.com/spf13/cobra"
	"github.com/stream-stack/dispatcher/pkg/config"
)

var address string

func InitFlags() {
	config.RegisterFlags(func(command *cobra.Command) {
		command.PersistentFlags().StringVar(&address, "Manager-Address", "0.0.0.0:8080", "manager address")
	})
}
