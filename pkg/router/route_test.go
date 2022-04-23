package router

import (
	"fmt"
	"github.com/ryszard/goskiplist/skiplist"
	"testing"
)

func TestAddPartition(t *testing.T) {
	intMap := skiplist.NewIntMap()
	intMap.Set(int(0), "0")
	intMap.Set(int(1), "0")
	intMap.Set(int(2), "0")
	intMap.Set(int(3), "0")
	iterator := intMap.Range(0, int(0))
	iterator.Previous()
	for iterator.Next() {
		fmt.Println(iterator.Key(), iterator.Value())
	}
}
