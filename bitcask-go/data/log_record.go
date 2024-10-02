package data

type LogRecordType = byte

const (
	LogRecordTypeNormal LogRecordType = iota
	LogRecordTypeDelete
)

// LogRecord 写入到数据文件的记录，以类似日志的形式追加到文件中
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id，描述数据存放到了哪个文件上
	Offset int64  // 偏移，描述数据在文件中的位置
}

// EncodeLogRecord 对log record进行编码，返回[]byte 和 长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
