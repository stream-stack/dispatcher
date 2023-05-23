package partition

import (
	"github.com/spf13/cobra"
	"github.com/stream-stack/dispatcher/pkg/config"
)

func InitFlags() {
	config.RegisterFlags(func(command *cobra.Command) {
	})
}
