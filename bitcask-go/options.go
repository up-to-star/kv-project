package bitcask_go

type Options struct {
	DirPath      string      // 数据库文件目录
	DataFileSize int64       // 数据文件的大小
	SyncWrites   bool        // 每次写入是否持久化
	IndexType    IndexerType // 索引类型
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)
