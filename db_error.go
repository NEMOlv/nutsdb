package nutsdb

import "errors"

var (
	// ErrDBClosed is returned when db is closed.
	// ErrDBClosed 当db关闭时返回
	ErrDBClosed = errors.New("db is closed")

	// ErrBucket is returned when bucket is not in the HintIdx.
	// ErrBucket 当bucket不在内存索引中时返回
	ErrBucket = errors.New("err bucket")

	// ErrFn is returned when fn is nil.
	// ErrFn 当fn为nil时返回
	ErrFn = errors.New("err fn")

	// ErrBucketNotFound is returned when looking for bucket that does not exist
	// ErrBucketNotFound 当bucket不存在时返回
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrDataStructureNotSupported is returned when pass a not supported data structure
	// ErrDataStructureNotSupported 当传入一个不支持的数据结构时返回
	ErrDataStructureNotSupported = errors.New("this data structure is not supported for now")

	// ErrDirLocked is returned when can't get the file lock of dir
	// ErrDirLocked 当在目录中获取不到文件锁时返回，即该目录已被加锁
	ErrDirLocked = errors.New("the dir of db is locked")

	// ErrDirUnlocked is returned when the file lock already unlocked
	// ErrDirUnlocked 当文件锁已经释放时返回，即该目录已被解锁
	ErrDirUnlocked = errors.New("the dir of db is unlocked")

	// ErrIsMerging is returned when merge in progress
	// ErrIsMerging 当执行merge进程时返回
	ErrIsMerging = errors.New("merge in progress")

	// ErrNotSupportMergeWhenUsingList is returned calling 'Merge' when using list
	// ErrNotSupportMergeWhenUsingList 当使用list数据结构时调用Merge函数返回
	// 即使用list数据结构无法支持合并
	ErrNotSupportMergeWhenUsingList = errors.New("not support merge when using list for now")

	// ErrRecordIsNil is returned when Record is nil
	// ErrRecordIsNil 当Record为nil时返回
	ErrRecordIsNil = errors.New("the record is nil")
)
