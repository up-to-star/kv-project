package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordTypeNormal LogRecordType = iota
	LogRecordTypeDelete
)

// crc type keySize valueSize
// 4 + 1 + 5 + 5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录，以类似日志的形式追加到文件中
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecord 的头部信息
type logRecordHeader struct {
	crc        uint32
	recordType LogRecordType // 标识 LogRecord 的类型
	keySize    uint32
	valueSize  uint32
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

// 对字节数组中的 Header 信息进行解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {

	return 0
}
