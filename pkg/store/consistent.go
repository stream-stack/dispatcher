package store

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
)

type Consistent struct {
	//排序的hash虚拟结点
	hashSortedNodes []uint32
	//虚拟结点对应的结点信息
	circle map[uint32]*PartitionSet
	//已绑定的结点
	nodes map[string]bool
}

func NewConsistent() *Consistent {
	return &Consistent{
		circle:          make(map[uint32]*PartitionSet),
		nodes:           make(map[string]bool),
		hashSortedNodes: make([]uint32, 0),
	}
}

func (c *Consistent) hashKey(key []byte) uint32 {
	return crc32.ChecksumIEEE(key)
}

func (c *Consistent) Add(set *PartitionSet) error {
	setName := set.Name
	if _, ok := c.nodes[setName]; ok {
		return errors.New("node already existed")
	}
	c.nodes[setName] = true
	//增加虚拟结点
	for i := 0; i < set.VirtualNodeCount; i++ {
		virtualKey := c.hashKey([]byte(setName + strconv.Itoa(i)))
		c.circle[virtualKey] = set
		c.hashSortedNodes = append(c.hashSortedNodes, virtualKey)
	}

	//虚拟结点排序
	sort.Slice(c.hashSortedNodes, func(i, j int) bool {
		return c.hashSortedNodes[i] < c.hashSortedNodes[j]
	})

	return nil
}

func (c *Consistent) GetNode(key []byte) (*PartitionSet, uint32) {
	hash := c.hashKey(key)
	i := c.getPosition(hash)

	u := c.hashSortedNodes[i]
	return c.circle[u], u
}

func (c *Consistent) getPosition(hash uint32) int {
	i := sort.Search(len(c.hashSortedNodes), func(i int) bool { return c.hashSortedNodes[i] >= hash })

	if i < len(c.hashSortedNodes) {
		if i == len(c.hashSortedNodes)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(c.hashSortedNodes) - 1
	}
}
