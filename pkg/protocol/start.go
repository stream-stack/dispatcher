package protocol

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"github.com/stream-stack/dispatcher/pkg/partition"
	"path/filepath"
	"plugin"
	"strings"
)

func Start(ctx context.Context) error {
	pluginDirs := viper.GetStringSlice("protocol-plugin-dir")
	total := len(pluginDirs)
	for i, dir := range pluginDirs {
		logrus.Debugf("[protocol][%d/%d]load %s protocol plugin...", i, total, dir)
		if err := StartDirPlugin(ctx, dir); err != nil {
			return err
		}
	}
	return nil
}

func StartDirPlugin(ctx context.Context, dir string) error {
	glob, err := filepath.Glob(filepath.Join(dir, "*.so"))
	if err != nil {
		return err
	}
	for _, s := range glob {
		if err := startPlugin(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func startPlugin(ctx context.Context, s string) error {
	open, err := plugin.Open(s)
	if err != nil {
		return err
	}
	lookup, err := open.Lookup("StartPlugin")
	if err != nil {
		if !strings.Contains(err.Error(), "not found") {
			logrus.Warnf("[protocol]lookup plugin %s StartPlugin function not found", s)
			return nil
		} else {
			logrus.Errorf("[protocol]lookup plugin %s StartPlugin function error:%v", s, err)
			return nil
		}
	}

	start, ok := lookup.(func(ctx context.Context, f func(ctx context.Context, event *v1.CloudEvent) error) error)
	if !ok {
		logrus.Errorf("[protocol]convert StartPlugin function error")
		return fmt.Errorf(`convert StartPlugin function error`)
	}
	return start(ctx, partition.StoreCloudEvent)
}
