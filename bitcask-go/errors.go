package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty            = errors.New("the key is empty")
	ErrIndexUpdateFailed     = errors.New("update index failed")
	ErrKeyNotFound           = errors.New("key not found")
	ErrDataFileNotFound      = errors.New("data file not found")
	ErrDataDirectoryCorrupt  = errors.New("data directory may be corrupted")
	ErrExceedMaxBatchNum     = errors.New("exceed the max batch num")
	ErrMergeIsProgress       = errors.New("merge is progress")
	ErrDatabaseIsUsing       = errors.New("the database directory is using by another process")
	ErrMergeRatioUnReached   = errors.New("merge ratio is unreached")
	ErrNoEnoughSpaceForMerge = errors.New("no enough space for merge")
)
