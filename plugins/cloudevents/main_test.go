package main

import (
	"context"
	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	http2 "github.com/cloudevents/sdk-go/v2/protocol/http"
	v1 "github.com/stream-stack/common/cloudevents.io/genproto/v1"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestStartPlugin(t *testing.T) {
	id := "100"
	tp := "test"
	source := "testSource"

	s := `{"test":1}`
	if err := StartPlugin(context.TODO(), func(ctx context.Context, event *v1.CloudEvent) []error {
		if event.Id != id {
			t.Error("id not equal")
		}

		if event.GetType() != tp {
			t.Error("type not equal")
		}

		if event.Source != source {
			t.Error("source not equal")
		}
		current := event.GetBinaryData()
		if !reflect.DeepEqual(s, string(current)) {
			t.Errorf("except: %s , current:%s", s, event.GetBinaryData())
		}
		return nil
	}); err != nil {
		t.Error(err)
	}
	reader := strings.NewReader(s)
	request, err := http.NewRequest("POST", "http://localhost:7080", reader)
	if err != nil {
		t.Fatal(err)
	}
	request.Header[http2.ContentType] = []string{v2.ApplicationJSON}
	request.Header["Ce-Id"] = []string{id}
	request.Header["Ce-Specversion"] = []string{event.CloudEventsVersionV1}
	request.Header["Ce-Type"] = []string{tp}
	request.Header["Ce-Source"] = []string{source}
	do, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if do.StatusCode != http.StatusOK {
		t.Errorf("bad http response status code:%v", do.StatusCode)
	}
}

//
//func TestEventConvert(t *testing.T) {
//	e := event.New(event.CloudEventsVersionV1)
//	e.SetID("100")
//	//e.SetSpecVersion(event.CloudEventsVersionV1)
//	e.SetType("test")
//	e.SetExtension("a", "test")
//	e.SetSource("testsource")
//	if err := e.SetData(v2.ApplicationJSON, `{
//  "test":1
//}`); err != nil {
//		t.Fatal(err)
//	}
//	pe := &v1.CloudEvent{}
//	json, err := e.MarshalJSON()
//	if err != nil {
//		t.Fatal(err)
//	}
//	if err = protojson.Unmarshal(json, pe); err != nil {
//		t.Fatal(err)
//	}
//
//}
