package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPlusTree && !db.seqNoExists && !db.isInitial {
		panic("cannot use WriteBatch with BPlusTree, seq no file not exists")
	}
	return &WriteBatch{
		options:       opts,
		mu:            &sync.Mutex{},
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 数据不存在直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordTypeDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 保证事务提交的串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取当前最新事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)
	positions := map[string]*data.LogRecordPos{}
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeqNo(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}
	finishedLogRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeqNo(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	_, err := wb.db.appendLogRecord(finishedLogRecord)
	if err != nil {
		return err
	}

	// 持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		err := wb.db.activeFile.Sync()
		if err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordTypeNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordTypeDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 清空暂存数据结构
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

func logRecordKeyWithSeqNo(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

func parseLogRecordKey(encKey []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(encKey)
	realKey := encKey[n:]
	return realKey, seqNo
}
