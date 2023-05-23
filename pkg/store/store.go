package store

import (
	"context"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
)

func SaveCloudEvent(ctx context.Context, event *v1.CloudEvent, clientAddr []string) error {
	// TODO: 多写,partition
	return nil
}
