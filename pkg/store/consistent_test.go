package store

import (
	"fmt"
	"github.com/stream-stack/common/partition"
	"testing"
)

func TestNewConsistent(t *testing.T) {
	c := NewConsistent()
	err := c.Add(&partition.Set{
		Name:             "test1",
		VirtualNodeCount: 100,
		Addrs:            []string{"localhost:8080"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Add(&partition.Set{
		Name:             "test2",
		VirtualNodeCount: 150,
		Addrs:            []string{"localhost:8082"},
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("test%d", i))
		node, u := c.GetNode(key)
		fmt.Println(node, u)
	}
}
