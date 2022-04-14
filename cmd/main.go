package main

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stream-stack/dispatcher/pkg/config"
	"github.com/stream-stack/dispatcher/pkg/manager"
	"github.com/stream-stack/dispatcher/pkg/recever"
	"github.com/stream-stack/dispatcher/pkg/router"
	"os"
	"os/signal"
)

func NewCommand() (*cobra.Command, context.Context, context.CancelFunc) {
	ctx, cancelFunc := context.WithCancel(context.TODO())
	command := &cobra.Command{
		Use:   ``,
		Short: ``,
		Long:  ``,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			go func() {
				c := make(chan os.Signal, 1)
				signal.Notify(c, os.Kill)
				<-c
				cancelFunc()
			}()
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			logrus.SetLevel(logrus.TraceLevel)
			if err := manager.StartManagerHttp(ctx); err != nil {
				return err
			}
			if err := manager.StartListWatcher(ctx); err != nil {
				return err
			}
			go manager.StartStoreSetConnManager(ctx)
			go router.StartRoute(ctx)
			if err := recever.StartReceive(ctx); err != nil {
				return err
			}

			<-ctx.Done()
			return nil
		},
	}
	manager.InitFlags()
	recever.InitFlags()

	viper.AutomaticEnv()
	viper.AddConfigPath(`.`)
	config.BuildFlags(command)

	return command, ctx, cancelFunc
}

func main() {
	command, _, _ := NewCommand()
	if err := command.Execute(); err != nil {
		panic(err)
	}
}
