package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecord{Fid: 1, Offset: 0})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecord{Fid: 2, Offset: 100})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecord{Fid: 1, Offset: 0})
	assert.True(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(0), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecord{Fid: 2, Offset: 100})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecord{Fid: 2, Offset: 101})
	assert.True(t, res3)
	pos3 := bt.Get([]byte("a"))
	t.Log(pos3)
	assert.Equal(t, uint32(2), pos3.Fid)
	assert.Equal(t, int64(101), pos3.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecord{Fid: 1, Offset: 1})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecord{Fid: 2, Offset: 3})
	assert.True(t, res3)
	res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)
}
