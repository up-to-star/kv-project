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
