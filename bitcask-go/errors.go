package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty           = errors.New("the key is empty")
	ErrIndexUpdateFailed    = errors.New("update index failed")
	ErrKeyNotFound          = errors.New("key not found")
	ErrDataFileNotFound     = errors.New("data file not found")
	ErrDataDirectoryCorrupt = errors.New("data directory may be corrupted")
)
