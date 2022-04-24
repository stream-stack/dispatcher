package router

import (
	"fmt"
	"github.com/huandu/skiplist"
	"testing"
)

func TestAddPartition(t *testing.T) {
	list := skiplist.New(skiplist.Uint64Desc)

	list.Set(uint64(0), "hello world")
	list.Set(uint64(3), 56)
	list.Set(uint64(5), 90.12)

	find := list.Find(uint64(4))
	fmt.Println(find)
	find = list.Find(uint64(3))
	fmt.Println(find)
	find = list.Find(uint64(2))
	fmt.Println(find)
	find = list.Find(uint64(0))
	fmt.Println(find)
}
