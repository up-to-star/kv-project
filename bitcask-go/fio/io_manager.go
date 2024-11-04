package fio

const DataFilePerm = 0644

type FileIOType byte

const (
	StandardFileIO FileIOType = iota
	MemoryMap
)

// IOManager 抽象IO接口，接入不同的IO类型，目前支持标准文件IO
type IOManager interface {
	// Read 从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	// Write 将字节数据写入到文件中
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取到文件的大小
	Size() (int64, error)
}

// NewIOManager 初始化 IOManager, 目前只支持标准文件IO
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFileIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMap(fileName)
	default:
		panic("unsupported io type")
	}
}
