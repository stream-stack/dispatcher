package partition

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"github.com/stream-stack/dispatcher/pkg/store"
	"testing"
	"time"
)

func TestStoreCloudEvent(t *testing.T) {
	logrus.SetLevel(logrus.TraceLevel)
	viper.Set("store-address-list", []string{"localhost:8080", "localhost:8081", "localhost:8082"})
	viper.Set("store-timeout", time.Second*5)
	todo := context.TODO()
	if err := store.StartStoreConn(todo); err != nil {
		t.Fatal(err)
	}
	if err := Start(todo); err != nil {
		t.Fatal(err)
	}
	AddPartition(&v1.Partition{
		Begin:        0,
		StoreAddress: []string{"localhost:8080", "localhost:8081", "localhost:8082"},
		CreateTime:   uint64(time.Now().Unix()),
	})

	es := StoreCloudEvent(todo, &v1.CloudEvent{
		Id:     "1",
		Source: "1",
		Type:   "test",
		Data:   &v1.CloudEvent_TextData{TextData: "1"},
	})
	fmt.Println(es)
}
