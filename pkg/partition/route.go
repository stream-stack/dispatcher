package partition

import (
	"context"
	"fmt"
	"github.com/huandu/skiplist"
	"github.com/sirupsen/logrus"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"github.com/stream-stack/dispatcher/pkg/store"
	"strconv"
)

func StoreCloudEvent(ctx context.Context, event *v1.CloudEvent) error {
	uid, err := strconv.ParseUint(event.GetId(), 10, 64)
	if err != nil {
		return err
	}
	OpCh <- func(ps *skiplist.SkipList) {
		find := ps.Find(uid)
		if find == nil {
			err = fmt.Errorf("partition not found")
			return
		}
		pt := find.Value.(*v1.Partition)
		logrus.Debugf(`[partition]find partition:%+v,eventId:%d`, pt, uid)
		err = store.SaveCloudEvent(ctx, event, pt.StoreAddress)
	}
	return err
}
