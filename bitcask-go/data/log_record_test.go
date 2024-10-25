package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常编码
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordTypeNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	//t.Log(res1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))
	// value 为空
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordTypeNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))
	t.Log(res2)
	// delete 情况
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordTypeDelete,
	}
	res3, n3 := EncodeLogRecord(rec3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
	t.Log(res3)
}

func TestDecodeLogRecord(t *testing.T) {
	headBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, n1 := decodeLogRecordHeader(headBuf1)
	t.Log(h1, n1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), n1)
	assert.Equal(t, uint32(2532332136), h1.crc)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)

	headBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, n2 := decodeLogRecordHeader(headBuf2)
	t.Log(h2, n2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), n2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	headBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	h3, n3 := decodeLogRecordHeader(headBuf3)
	t.Log(h3, n3)
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), n3)
	assert.Equal(t, uint32(290887979), h3.crc)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valueSize)
	assert.Equal(t, byte(1), h3.recordType)
}

func TestGetLogRecord(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordTypeNormal,
	}

	headBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}

	crc := getLogRecordCRC(rec1, headBuf1[crc32.Size:])
	assert.Equal(t, uint32(2532332136), crc)

	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordTypeNormal,
	}

	headBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc = getLogRecordCRC(rec2, headBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc)

	headBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordTypeDelete,
	}
	crc = getLogRecordCRC(rec3, headBuf3[crc32.Size:])
	assert.Equal(t, uint32(290887979), crc)
}
