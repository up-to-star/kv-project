package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 定义抽象索引接口，方便后续接入其他索引数据结构
type Indexer interface {
	// Put 向索引中存储数据对应的位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 获取索引中数据对应的位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应的位置信息
	Delete(key []byte) bool

	// Iterator 返回迭代器
	Iterator(reverse bool) Iterator

	// Size 返回索引中有多少条数据
	Size() int
}

type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基树索引
	ART
)

func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		// TODO
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点
	Rewind()

	// Seek 根据传入 key 查找到第一个大于（或小于）等于的目标key，从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经遍历完了所有的key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 key 数据
	Key() []byte

	// Value 当前遍历位置的 value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放响应的资源
	Close()
}
