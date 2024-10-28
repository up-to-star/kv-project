package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDb_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-2")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())

	value, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), value)
}

func TestDb_Iterator_Multi_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-4")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("atest"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("btest"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ssest"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ccest"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("xxest"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("aaest"), utils.RandomValue(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	for ; iterator.Valid(); iterator.Next() {
		assert.NotNil(t, iterator.Key())
	}

	iterator.Rewind()
	for iterator.Seek([]byte("c")); iterator.Valid(); iterator.Next() {
		assert.NotNil(t, iterator.Key())
	}

	// 反向迭代
	iterOptions := DefaultIteratorOptions
	iterOptions.Reverse = true
	iter := db.NewIterator(iterOptions)
	assert.NotNil(t, iter)
	assert.Equal(t, true, iter.Valid())
	for ; iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
	}

	iter.Rewind()
	for iter.Seek([]byte("c")); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
	}

	// 指定 prefix
	iterOptions1 := DefaultIteratorOptions
	iterOptions1.Prefix = []byte("a")
	iter1 := db.NewIterator(iterOptions1)
	assert.NotNil(t, iter1)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		t.Log("key = ", string(iter1.Key()))
	}
}
