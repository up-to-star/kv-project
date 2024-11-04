package fio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"path/filepath"
	"testing"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/tmp", "mmap-a.data")
	defer destroyFile(path)

	mmapIO, err := NewMMap(path)
	assert.Nil(t, err)
	assert.NotNil(t, mmapIO)

	// 文件为空
	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	assert.Equal(t, 0, n1)
	assert.Equal(t, io.EOF, err)

	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("aa"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("bb"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("cc"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMap(path)
	assert.Nil(t, err)
	b2 := make([]byte, 10)
	n2, err := mmapIO2.Read(b2, 0)
	assert.Equal(t, 6, n2)
	assert.Equal(t, io.EOF, err)
	size, err := mmapIO2.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(6), size)

	b2 = make([]byte, 2)
	n3, err := mmapIO2.Read(b2, 0)
	t.Log(n3)
	t.Log(string(b2))
}
