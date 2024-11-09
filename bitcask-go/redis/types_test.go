package redis

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestRedisDataStructure_Get(t *testing.T) {
	options := bitcask.DefaultOptions
	options.DirPath, _ = os.MkdirTemp("", "TestRedisDataStructure_Get")
	rds, err := NewRedisDataStruct(options)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(128))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(128))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)
	time.Sleep(time.Second * 5)
	val2, err = rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(val2))
}

func TestRedisDataStructure_Del(t *testing.T) {
	options := bitcask.DefaultOptions
	options.DirPath, _ = os.MkdirTemp("", "TestRedisDataStructure_Del")
	rds, err := NewRedisDataStruct(options)
	assert.Nil(t, err)
	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(128))
	assert.Nil(t, err)
	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)
	_, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)

}

func TestRedisDataStructure_Type(t *testing.T) {
	options := bitcask.DefaultOptions
	options.DirPath, _ = os.MkdirTemp("", "TestRedisDataStructure_Type")
	rds, err := NewRedisDataStruct(options)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(128))
	assert.Nil(t, err)

	dataType, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, dataType)
}

func TestRedisDataStructure_HGet(t *testing.T) {
	options := bitcask.DefaultOptions
	options.DirPath, _ = os.MkdirTemp("", "TestRedisDataStructure_HGet")
	rds, err := NewRedisDataStruct(options)
	assert.Nil(t, err)
	ok, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(128))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(128))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(128))
	assert.Nil(t, err)
	assert.False(t, ok)

	val, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	val, err = rds.HGet(utils.GetTestKey(1), []byte("field3"))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
	assert.Equal(t, 0, len(val))
}

func TestRedisDataStructure_HDel(t *testing.T) {
	options := bitcask.DefaultOptions
	options.DirPath, _ = os.MkdirTemp("", "TestRedisDataStructure_HDel")
	rds, err := NewRedisDataStruct(options)
	assert.Nil(t, err)

	ok, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(128))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(128))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.HDel(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.True(t, ok)
}
