package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid CRC value, log record maybe corrupted")
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
)

type DataFile struct {
	FileId      uint32        // 文件ID
	WriteOffset int64         // 文件写到了哪个位置
	IoManager   fio.IOManager // io 管理
}

// OpenDataFile 打开数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	// 初始化 IOManager 管理接口
	return newDataFile(fileName, fileId)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func newDataFile(fileName string, fileId uint32) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{FileId: fileId, WriteOffset: 0, IoManager: ioManager}, nil
}

// OpenHintFile 打开 hint 索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)
}

func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0)
}

func (df *DataFile) Sync() error {

	return df.IoManager.Sync()
}

// WriteHintRecord 写入索引信息到 hint 文件中
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)

	return df.Write(encRecord)
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(n)
	return nil
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// ReadLogRecord 根据 offset 从数据文件中读取logRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 读取的最大 header 长度超过文件长度， 则只读取到文件末尾
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}
	// 读取 Header 信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := decodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出 key 和 value 的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize
	logRecord := &LogRecord{Type: header.recordType}
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		// 解出 key 和 value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	// 校验数据的有效性
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}
