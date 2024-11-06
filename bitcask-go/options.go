package bitcask_go

import "os"

type Options struct {
	DirPath       string      // 数据库文件目录
	DataFileSize  int64       // 数据文件的大小
	SyncWrites    bool        // 每次写入是否持久化
	BytesPerSync  uint        // 累计写到这个阈值，再持久化
	IndexType     IndexerType // 索引类型
	MMapAtStartup bool
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	// 遍历以 prefix 为前缀的 key, 默认为空
	Prefix []byte
	// 是否反向, 默认 false 是正向
	Reverse bool
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:       os.TempDir(),
	DataFileSize:  256 * 1024 * 1024,
	SyncWrites:    false,
	BytesPerSync:  0,
	IndexType:     ART,
	MMapAtStartup: true,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

type WriteBatchOptions struct {
	MaxBatchNum uint
	SyncWrites  bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
