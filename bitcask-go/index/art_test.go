package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	res := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.Nil(t, res)
	res = art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res)
	res = art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Nil(t, res)
	res = art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 4})
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 3}, res)
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	pos := art.Get([]byte("key-1"))
	assert.NotNil(t, pos)

	pos = art.Get([]byte("not-exist"))
	assert.Nil(t, pos)
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 2})
	pos = art.Get([]byte("key-2"))
	assert.NotNil(t, pos)
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 3})
	pos = art.Get([]byte("key-3"))
	assert.NotNil(t, pos)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	res, ok := art.Delete([]byte("key"))
	assert.Equal(t, false, ok)
	assert.Nil(t, res)
	res, ok = art.Delete([]byte("key-1"))
	assert.Equal(t, true, ok)
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 1}, res)
	pos := art.Get([]byte("key-1"))
	assert.Nil(t, pos)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	assert.Equal(t, 0, art.Size())
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Equal(t, 2, art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 2})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 3})
	art.Put([]byte("key-4"), &data.LogRecordPos{Fid: 1, Offset: 4})
	iter := art.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		record := iter.Value()
		assert.NotNil(t, record)
	}

	iter = art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		record := iter.Value()
		t.Log(record)
	}
}
