package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_WriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	// 没有 commit
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// commit
	err = wb.Commit()
	assert.Nil(t, err)

	val, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)
	val, err = db.Get(utils.GetTestKey(1))
	t.Log(val)
}

func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-3")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)

	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(3), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	_, err = db2.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	assert.Equal(t, uint64(2), db2.seqNo)

}
