package data

import "bitcask-go/fio"

const DataFileNameSuffix = ".data"

type DataFile struct {
	FileId      uint32        // 文件ID
	WriteOffset int64         // 文件写到了哪个位置
	IoManager   fio.IOManager // io 管理
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
