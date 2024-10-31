package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	openDataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, openDataFile)

	openDataFile1, err1 := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err1)
	assert.NotNil(t, openDataFile1)
	t.Log(os.TempDir())

	openDataFile2, err2 := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err2)
	assert.NotNil(t, openDataFile2)
	t.Log(os.TempDir())
}

func TestDataFile_Write(t *testing.T) {
	openDataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, openDataFile)

	err = openDataFile.Write([]byte("aaa"))
	assert.Nil(t, err)
	err = openDataFile.Write([]byte("bbb"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	openDataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, openDataFile)

	err = openDataFile.Write([]byte("aaa"))
	assert.Nil(t, err)
	err = openDataFile.Write([]byte("bbb"))
	assert.Nil(t, err)

	err = openDataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	openDataFile, err := OpenDataFile(os.TempDir(), 123)
	assert.Nil(t, err)
	assert.NotNil(t, openDataFile)

	err = openDataFile.Write([]byte("aaa"))
	assert.Nil(t, err)
	err = openDataFile.Write([]byte("bbb"))
	assert.Nil(t, err)

	err = openDataFile.Sync()
	assert.Nil(t, err)

	err = openDataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	openDataFile, err := OpenDataFile(os.TempDir(), 888)
	assert.Nil(t, err)
	assert.NotNil(t, openDataFile)

	// 只有一条log record
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv go"),
	}
	res1, size1 := EncodeLogRecord(rec1)
	err = openDataFile.Write(res1)
	assert.Nil(t, err)

	readRec1, readSize, err := openDataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize)
	//t.Log(size1)

	// 多条记录
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new log record go"),
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = openDataFile.Write(res2)
	assert.Nil(t, err)
	readRec2, readSize2, err := openDataFile.ReadLogRecord(24)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)
	//t.Log(size2)
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("value"),
		Type:  LogRecordTypeDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = openDataFile.Write(res3)
	assert.Nil(t, err)
	//t.Log(size3)
	readRec3, readSize3, err := openDataFile.ReadLogRecord(size1 + size2)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, readSize3)

}
