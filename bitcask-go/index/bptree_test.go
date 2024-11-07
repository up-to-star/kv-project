package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)
	res := tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.Nil(t, res)
	res = tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res)
	res = tree.Put([]byte("acd"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Nil(t, res)

	res = tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 4})
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 1}, res)

}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 1})
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 2})
	tree.Put([]byte("acd"), &data.LogRecordPos{Fid: 1, Offset: 3})
	pos := tree.Get([]byte("aac"))
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 2}, pos)

	pos = tree.Get([]byte("not-exist"))
	assert.Nil(t, pos)
}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 1})
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 2})
	tree.Put([]byte("acd"), &data.LogRecordPos{Fid: 1, Offset: 3})
	res, ok := tree.Delete([]byte("not-exist"))
	assert.False(t, ok)
	assert.Nil(t, res)
	res, ok = tree.Delete([]byte("abc"))
	assert.True(t, ok)
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 1}, res)
}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)
	assert.Equal(t, 0, tree.Size())
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 1})
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 1, Offset: 2})
	tree.Put([]byte("acd"), &data.LogRecordPos{Fid: 1, Offset: 3})
	size := tree.Size()
	assert.Equal(t, 3, size)

	tree.Delete([]byte("aac"))
	size = tree.Size()
	assert.Equal(t, 2, size)
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 1})
	tree.Put([]byte("ade"), &data.LogRecordPos{Fid: 1, Offset: 2})
	tree.Put([]byte("acd"), &data.LogRecordPos{Fid: 1, Offset: 3})
	tree.Put([]byte("add"), &data.LogRecordPos{Fid: 1, Offset: 4})
	tree.Put([]byte("del"), &data.LogRecordPos{Fid: 1, Offset: 5})
	iter := tree.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}
}
