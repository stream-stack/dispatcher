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

func StoreCloudEvent(ctx context.Context, event *v1.CloudEvent) []error {
	var errs = make([]error, 0)
	//TODO:支持其他id类型
	uid, err := strconv.ParseUint(event.GetId(), 10, 64)
	if err != nil {
		errs = append(errs, err)
		return errs
	}
	errsCh := make(chan error)
	f := func(ps *skiplist.SkipList) {
		defer close(errsCh)
		find := ps.Find(uid)
		if find == nil {
			err = fmt.Errorf("partition not found")
			errsCh <- err
			return
		}
		pt := find.Value.(*v1.Partition)
		logrus.Debugf(`[partition]find partition:%+v,eventId:%d`, pt, uid)
		es := store.SaveCloudEvent(ctx, event, pt.StoreAddress)
		for _, e := range es {
			errsCh <- e
		}
	}
	OpCh <- f
	for e := range errsCh {
		errs = append(errs, e)
	}
	return errs
}
