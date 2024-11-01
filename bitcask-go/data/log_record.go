package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordTypeNormal LogRecordType = iota
	LogRecordTypeDeleted
	LogRecordTxnFinished
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

type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id，描述数据存放到了哪个文件上
	Offset int64  // 偏移，描述数据在文件中的位置
}

// EncodeLogRecord 对log record进行编码，返回[]byte 和 长度
// crc | type | key size | value size | key | value
//
//	4  |   1  | 变长 max = 5 |         |  bc  | bc
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)
	header[4] = logRecord.Type
	var index = 5

	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)
	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)
	crc := crc32.ChecksumIEEE(encBytes[4:])
	// 小端序存储
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	return encBytes, int64(size)
}

func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	return buf
}

func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])
	return &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: offset,
	}
}

// 对字节数组中的 Header 信息进行解码
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	var index = 5
	keySize, n := binary.Varint(buf[index:])
	index += n
	header.keySize = uint32(keySize)

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n
	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
