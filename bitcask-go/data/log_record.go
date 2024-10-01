package data

// LogRecord 数据内存索引，描述数据在磁盘上的位置
type LogRecord struct {
	Fid    uint32 // 文件id，描述数据存放到了哪个文件上
	Offset int64  // 偏移，描述数据在文件中的位置
}
