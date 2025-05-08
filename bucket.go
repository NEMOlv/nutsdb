package nutsdb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// 声明bucket元数据大小变量
var BucketMetaSize int64

// 声明bucket_id大小和 数据结构大小
// 数据结构包括：Key-Value pair (BTree)、List、Set、ZSet
const (
	IdSize = 8
	DsSize = 2
)

// 声明bucket操作类型，包括：插入、更新、删除
type BucketOperation uint16

const (
	BucketInsertOperation BucketOperation = iota + 1
	BucketUpdateOperation
	BucketDeleteOperation
)

// crc无效错误
var ErrBucketCrcInvalid = errors.New("bucket crc invalid")

// init 初始化BucketMetaSize
func init() {
	BucketMetaSize = GetDiskSizeFromSingleObject(BucketMeta{})
}

// BucketMeta stores the Meta info of a Bucket. E.g. the size of bucket it store in disk.
// BucketMeta 存储bucket的元数据
type BucketMeta struct {
	// Crc: 校验和
	Crc uint32
	// Op: Mark the latest operation (e.g. delete, insert, update) for this bucket.
	// Op: 记录最后一次操作
	Op BucketOperation
	// Size: 有效数据的大小
	// Size: the size of payload.
	Size uint32
}

// Bucket is the disk structure of bucket
// Bucket是桶文件的磁盘结构
type Bucket struct {
	// Meta: the metadata for this bucket
	// Meta: bucket的元数据
	Meta *BucketMeta
	// Id: is the marker for this bucket, every bucket creation activity will generate a new Id for it.
	// for example. If you have a bucket called "bucket_1", and you just delete bucket and create it again.
	// the last bucket will have a different Id from the previous one.
	// Id: ID是bucket的标记，每创建一个bucket都会生成一个新ID
	// 例如：如果你有一个名为"bucket_1"的bucket，你只是删除桶并再次创建，最新的bucket将会和之前的bucket拥有不一样的ID
	Id BucketId
	// Ds: the data structure for this bucket. (List, Set, SortSet, String)
	// Ds: bucket的数据结构
	Ds Ds
	// Name: the name of this bucket.
	// Name: bucket的名字
	Name string
}

// Decode: CRC | op | size
//
//	4B    2B    4B
func (meta *BucketMeta) Decode(bytes []byte) {
	_ = bytes[BucketMetaSize-1]
	crc := binary.LittleEndian.Uint32(bytes[:4])
	op := binary.LittleEndian.Uint16(bytes[4:6])
	size := binary.LittleEndian.Uint32(bytes[6:10])
	meta.Crc = crc
	meta.Size = size
	meta.Op = BucketOperation(op)
}

// Encode: Meta | BucketId | Ds | BucketName
// Encode:
func (b *Bucket) Encode() []byte {
	entrySize := b.GetEntrySize()
	buf := make([]byte, entrySize)
	b.Meta.Size = uint32(b.GetPayloadSize())
	binary.LittleEndian.PutUint16(buf[4:6], uint16(b.Meta.Op))
	binary.LittleEndian.PutUint32(buf[6:10], b.Meta.Size)
	binary.LittleEndian.PutUint64(buf[BucketMetaSize:BucketMetaSize+IdSize], uint64(b.Id))
	binary.LittleEndian.PutUint16(buf[BucketMetaSize+IdSize:BucketMetaSize+IdSize+DsSize], uint16(b.Ds))
	copy(buf[BucketMetaSize+IdSize+DsSize:], b.Name)
	c32 := crc32.ChecksumIEEE(buf[4:])
	b.Meta.Crc = c32
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// Decode : Meta | BucketId | Ds | BucketName
func (b *Bucket) Decode(bytes []byte) error {
	// parse the payload
	id := binary.LittleEndian.Uint64(bytes[:IdSize])
	ds := binary.LittleEndian.Uint16(bytes[IdSize : IdSize+DsSize])
	name := bytes[IdSize+DsSize:]
	b.Id = id
	b.Name = string(name)
	b.Ds = ds
	return nil
}

// 获得元数据大小+有效数据大小
func (b *Bucket) GetEntrySize() int {
	return int(BucketMetaSize) + b.GetPayloadSize()
}

// 获得CRC值
func (b *Bucket) GetCRC(headerBuf []byte, dataBuf []byte) uint32 {
	crc := crc32.ChecksumIEEE(headerBuf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, dataBuf)
	return crc
}

// 获得有效数据大小
func (b *Bucket) GetPayloadSize() int {
	return IdSize + DsSize + len(b.Name)
}
