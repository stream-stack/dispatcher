package protocol

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"github.com/stream-stack/dispatcher/pkg/store"
	"path/filepath"
	"plugin"
	"strings"
)

func Start(ctx context.Context) error {
	pluginDirs := viper.GetStringSlice("ProtocolPluginDir")
	total := len(pluginDirs)
	for i, dir := range pluginDirs {
		if err := StartDirPlugin(ctx, dir); err != nil {
			return err
		}
		logrus.Debugf("[protocol][%d/%d]load dir:%v protocol plugin finish", i+1, total, dir)
	}
	return nil
}

func StartDirPlugin(ctx context.Context, dir string) error {
	join := filepath.Join(dir, "*", "*.so")
	glob, err := filepath.Glob(join)
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

	start, ok := lookup.(func(ctx context.Context, f func(ctx context.Context, event *v1.CloudEvent) []error) error)
	if !ok {
		logrus.Errorf("[protocol]convert StartPlugin function error")
		return fmt.Errorf(`convert StartPlugin function error`)
	}
	return start(ctx, store.SaveCloudEvent)
}
