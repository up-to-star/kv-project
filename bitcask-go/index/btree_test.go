package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 0})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 2, Offset: 100})
	assert.Nil(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 11, Offset: 200})
	assert.Equal(t, &data.LogRecordPos{Fid: 2, Offset: 100}, res3)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 0})
	assert.Nil(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(0), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 2, Offset: 100})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 2, Offset: 101})
	assert.Equal(t, &data.LogRecordPos{Fid: 2, Offset: 100}, res3)
	pos3 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(2), pos3.Fid)
	assert.Equal(t, int64(101), pos3.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.Nil(t, res1)

	pos, res2 := bt.Delete(nil)
	assert.True(t, res2)
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 1}, pos)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 2, Offset: 3})
	assert.Nil(t, res3)
	pos, res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)
	assert.Equal(t, &data.LogRecordPos{Fid: 2, Offset: 3}, pos)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	bt1.Put([]byte("cache"), &data.LogRecordPos{Fid: 1, Offset: 0})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	bt1.Put([]byte("aaaa"), &data.LogRecordPos{Fid: 1, Offset: 0})
	bt1.Put([]byte("bbbb"), &data.LogRecordPos{Fid: 1, Offset: 0})
	bt1.Put([]byte("cccc"), &data.LogRecordPos{Fid: 1, Offset: 0})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// 测试 Seek
	iter5 := bt1.Iterator(false)
	iter5.Seek([]byte("cc"))
	t.Log(string(iter5.Key()))
}
