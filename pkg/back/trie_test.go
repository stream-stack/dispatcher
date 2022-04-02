package back

import (
	"fmt"
	"github.com/stream-stack/dispatcher/pkg/manager/protocol"
	"testing"
)

func TestAddNode(t *testing.T) {
	err := AddNode("123[0-9]{5}", protocol.Store{
		Name:      "1",
		Namespace: "1",
		Uris:      nil,
	})
	if err != nil {
		panic(err)
	}
	find, b := Find("12345555")
	fmt.Println(find, b)
	find, b = Find("1234555")
	fmt.Println(find, b)
	find, b = Find("12445555")
	fmt.Println(find, b)
}
