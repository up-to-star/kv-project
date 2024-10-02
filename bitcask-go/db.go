package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"sync"
)

type DB struct {
	options    Options
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃的数据文件，可以写入
	oldFiles   map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index      index.Indexer             // 内存索引
}

// Put 写入key value 数据，key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordTypeNormal,
	}

	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	// 从内存中把数据信息拿出来
	logRecordPos := db.index.Get(key)
	// 如果 kye 不在内存索引中，key 不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件ID找到对应的数据文件
	var dataFile *data.DataFile
	if logRecordPos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFiles[logRecordPos.Fid]
	}
	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 找到数据文件，根据 offset 读取数据
	logRecord, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordTypeDelete {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	encRecord, size := data.EncodeLogRecord(logRecord)
	if db.activeFile.WriteOffset+size > db.options.DataFileSize {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 将当前活跃文件转化为一个旧的文件
		db.oldFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// 设置当前的活跃文件
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0

	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
