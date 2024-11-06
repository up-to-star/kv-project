package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/fio"
	"bitcask-go/index"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	seqNoKey      = "seq.no"
	fileFlockName = "flock"
)

type DB struct {
	options     Options
	mu          *sync.RWMutex
	fileIds     []int                     // 文件 di, 只在加载索引的时候使用
	activeFile  *data.DataFile            // 当前活跃的数据文件，可以写入
	oldFiles    map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index       index.Indexer             // 内存索引
	seqNo       uint64                    // 事务序列号, 全局递增
	isMerging   bool                      // 是不是在merge
	seqNoExists bool                      // 标识存储事务序列号的文件是否存在
	isInitial   bool                      // 标识第一次初始化数据目录
	fileLock    *flock.Flock              // 文件锁
	bytesWrite  uint                      // 累计写了多少个字节
}

// Open 打开 bitcask 存储引擎实例
func Open(options Options) (*DB, error) {
	// 检查配置选项
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	var isInitial bool
	// 如果文件不存在，则创建文件
	_, err := os.Stat(options.DirPath)
	if os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileFlockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	// 初始化 DB 实例
	db := &DB{
		options:   options,
		mu:        new(sync.RWMutex),
		oldFiles:  make(map[uint32]*data.DataFile),
		index:     index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial: isInitial,
		fileLock:  fileLock,
	}

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}
	// 加载对应的数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	if options.IndexType != BPlusTree {
		// 从 hint 索引中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}
		// 从数据文件加载索引
		if err := db.loadIndexFromDataFile(); err != nil {
			return nil, err
		}

		if db.options.MMapAtStartup {
			err := db.resetIoType()
			if err != nil {
				return nil, err
			}
		}
	}

	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOffset = size
		}
	}

	return db, nil
}

// Put 写入key value 数据，key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeqNo(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordTypeNormal,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 检查 key 是否存在，如果不存在直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造 LogRecord, 标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeqNo(key, nonTransactionSeqNo),
		Type: data.LogRecordTypeDeleted,
	}

	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	// 从内存索引中将对应的key删除
	ok := db.index.Delete(key)
	if !ok {
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

	return db.getValueByPosition(logRecordPos)
}

func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failked to unlock directory: %v", err))
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.index.Close(); err != nil {
		return err
	}

	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return nil
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}

	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, oldFile := range db.oldFiles {
		if err := oldFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.activeFile.Sync(); err != nil {
		return err
	}
	return nil
}

// ListKeys 获取数据库中所有的 key
func (db *DB) ListKeys() [][]byte {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, db.index.Size())

	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有数据, 并执行用户指定的操作, 函数返回false 退出遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}

		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
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
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordTypeDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
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
	db.bytesWrite += uint(size)
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}

	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
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

	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardFileIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 加载数据文件
func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中的所有文件，找到.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录可能被破坏掉了
			if err != nil {
				return ErrDataDirectoryCorrupt
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 id 排序，从小到大加载数据文件
	sort.Ints(fileIds)
	db.fileIds = fileIds
	// 遍历文件id，打开对应的数据文件
	for i, fid := range fileIds {
		ioType := fio.StandardFileIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}
		// 最后一个文件表示活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.oldFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// 从数据文件加载索引
func (db *DB) loadIndexFromDataFile() error {
	// 空的数据库，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	hasMerged, nonMergeFileId := false, uint32(0)
	mergedFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergedFileName); err == nil {
		fid, err := db.getNonMergeFileId(mergedFileName)
		if err != nil {
			return err
		}
		hasMerged = true
		nonMergeFileId = fid
	}
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordTypeDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup!")
		}
	}

	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo
	// 遍历文件id，处理文件当中的内容
	for i, fid := range db.fileIds {
		fileId := uint32(fid)
		if hasMerged && fileId < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.oldFiles[fileId]
		}
		// 处理文件当中的内容
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构建内存索引，保存到内存索引中
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						realKey, _ = parseLogRecordKey(txnRecord.Record.Key)
						updateIndex(realKey, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}
			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			offset += size
		}
		// 活跃文件 offset 更新
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOffset = offset
		}
	}
	db.seqNo = currentSeqNo
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("data file size must be positive")
	}

	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(fileName)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}

	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoExists = true
	return os.Remove(fileName)
}

func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}
	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFileIO); err != nil {
		return err
	}

	for _, dataFile := range db.oldFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFileIO); err != nil {
			return err
		}
	}
	return nil
}
