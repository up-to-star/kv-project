package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 定义抽象索引接口，方便后续接入其他索引数据结构
type Indexer interface {
	// Put 向索引中存储数据对应的位置信息
	Put(key []byte, pos *data.LogRecord) bool

	// Get 获取索引中数据对应的位置信息
	Get(key []byte) *data.LogRecord

	// Delete 根据 key 删除对应的位置信息
	Delete(key []byte) bool
}

type Item struct {
	key []byte
	pos *data.LogRecord
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
