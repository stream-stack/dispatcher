package partition

import (
	"context"
	"github.com/huandu/skiplist"
	"github.com/sirupsen/logrus"
	"github.com/stream-stack/common"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"github.com/stream-stack/dispatcher/pkg/store"
)

var OpCh = make(chan func(partitions *skiplist.SkipList), 1)
var partitionList = skiplist.New(skiplist.Uint64Desc)

func Start(ctx context.Context) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case op := <-OpCh:
				op(partitionList)
			}
		}
	}()
	total := len(store.StoreClients)
	current := 1
	for _, c := range store.StoreClients {
		logrus.Debugf("[partition][%d/%d]begin subscribe partition to store address:%v", current, total, c)
		storeClient := v1.NewSubscriptionClient(c)
		if err := startPartitionSubscription(ctx, storeClient); err != nil {
			return err
		}
		current++
	}
	return nil
}

func AddPartition(partition *v1.Partition) {
	OpCh <- func(partitions *skiplist.SkipList) {
		logrus.Debugf("[protocol]add partition %+v", partition)
		find := partitions.Find(partition.Begin)
		if find != nil {
			o := find.Value.(*v1.Partition)
			if o == nil {
				return
			}
			if o.CreateTime < partition.CreateTime {
				logrus.Infof("[protocol]partition exist , but createTime less current partition createTime,exist paratition: %+v,current partition:%+v", o, partition)
				partitions.Remove(partition.Begin)
			}
		}

		partitions.Set(partition.Begin, partition)
		total := partitions.Len()
		logrus.Debugf("[partition]add partition end , current partition length: %v", total)
	}
}

func startPartitionSubscription(ctx context.Context, client v1.SubscriptionClient) error {
	value := common.PartitionStoreTypeValue
	subscribe, err := client.Subscribe(ctx, &v1.SubscribeRequest{
		Type: &value,
	})
	if err != nil {
		logrus.Errorf("[partition]subscribe partition error:%v", err)
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-subscribe.Context().Done():
				return
			default:
				recv, err := subscribe.Recv()
				if err != nil {
					logrus.Errorf("[partition]store server recv error:%v", err)
					return
				}
				logrus.Debugf("[partition]recv partition data:%+v", recv)
				p := &v1.Partition{}
				err = recv.Event.GetProtoData().UnmarshalTo(p)
				if err != nil {
					logrus.Errorf("[partition]unmarshal cloudEvent:%v to Partition error:%v ,skip this partition", recv.Offset, err)
					continue
				}
				AddPartition(p)
			}
		}
	}()
	return nil
}
