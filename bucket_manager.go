package nutsdb

import (
	"errors"
	"os"
)

// 找不到bucket的错误
var ErrBucketNotExist = errors.New("bucket not exist")

// bucket存储文件名
const BucketStoreFileName = "bucket.Meta"

type Ds = uint16
type BucketId = uint64
type BucketName = string

// 通过BucketName_map可以找到Ds_map，通过Ds_map可以找到桶ID
// 举例：
//
//	map['bucket_1']['BTree'] -> 0
//	map['bucket_1']['Set'] -> 1
//	map['bucket_2']['BTree'] -> 2
type IDMarkerInBucket map[BucketName]map[Ds]BucketId

// 通过BucketId_map可以找到Bucket指针(实例)
// 举例：
//
//	map[0] -> bucket_ptr_0
//	map[3] -> bucket_ptr_3
type InfoMapperInBucket map[BucketId]*Bucket

// BucketManager 桶管理器
type BucketManager struct {
	// 桶元数据存储文件
	fd *os.File
	// BucketInfoMapper BucketID => Bucket itself
	// BucketInfoMapper 通过BucketID获取Bucket实例
	BucketInfoMapper InfoMapperInBucket
	// BucketIDMarker BucketName + Ds => BucketID
	// BucketIDMarker 通过BucketName+Ds获取BucketID
	BucketIDMarker IDMarkerInBucket

	// IDGenerator helps generates an ID for every single bucket
	// IDGenerator 为每个桶生成一个ID
	Gen *IDGenerator
}

// NewBucketManager 新建一个BucketManager(桶管理器)
func NewBucketManager(dir string) (*BucketManager, error) {
	// 新建桶管理器
	bm := &BucketManager{
		BucketInfoMapper: map[BucketId]*Bucket{},
		BucketIDMarker:   map[BucketName]map[Ds]BucketId{},
	}

	// 获取桶元数据文件
	bucketFilePath := dir + "/" + BucketStoreFileName
	_, err := os.Stat(bucketFilePath)
	mode := os.O_RDWR
	// 如果err不为空，将 os.O_RDWR 与 os.O_CREATE 进行按位或运算
	// 按位或运算后，mode 的值会变为 os.O_RDWR | os.O_CREATE，即同时具有读写模式和创建模式
	if err != nil {
		mode |= os.O_CREATE
	}
	fd, err := os.OpenFile(bucketFilePath, mode, os.ModePerm)
	if err != nil {
		return nil, err
	}

	bm.fd = fd
	bm.Gen = &IDGenerator{currentMaxId: 0}
	return bm, nil
}

// bucketSubmitRequest 桶提交请求
type bucketSubmitRequest struct {
	// 数据结构
	ds Ds
	// 桶名
	name BucketName
	// 桶实例
	bucket *Bucket
}

// SubmitPendingBucketChange 提交待写桶数组
func (bm *BucketManager) SubmitPendingBucketChange(reqs []*bucketSubmitRequest) error {
	bytes := make([]byte, 0)
	for _, req := range reqs {
		// 对桶实例编码
		bs := req.bucket.Encode()
		// 将桶加入字节数组
		bytes = append(bytes, bs...)
		// update the marker info
		// 更新 marker 信息
		// 如果marker中找不到该桶，则将该桶存储进BucketIDMarker
		if _, exist := bm.BucketIDMarker[req.name]; !exist {
			bm.BucketIDMarker[req.name] = map[Ds]BucketId{}
		}
		// recover maxid otherwise new bucket start from 1 again
		// 恢复 maxid，否则桶重新从 1 开始
		bm.Gen.CompareAndSetMaxId(req.bucket.Id)
		// 如果是插入操作，则使用BucketInfoMapper存储Bucket实例，使用BucketIDMarker存储BucketID
		// 如果是删除操作，则删除BucketInfoMapper中存储的Bucket实例，删除BucketIDMarker存储的BucketID
		switch req.bucket.Meta.Op {
		case BucketInsertOperation:
			bm.BucketInfoMapper[req.bucket.Id] = req.bucket
			bm.BucketIDMarker[req.name][req.bucket.Ds] = req.bucket.Id
		case BucketDeleteOperation:
			// 如果len(bm.BucketIDMarker[req.name]) == 1，代表只有一个数据结构使用了该桶名，直接删除即可
			// 否则要删除对应数据结构中的桶ID
			if len(bm.BucketIDMarker[req.name]) == 1 {
				delete(bm.BucketIDMarker, req.name)
			} else {
				delete(bm.BucketIDMarker[req.name], req.bucket.Ds)
			}
			// 删除桶实例
			delete(bm.BucketInfoMapper, req.bucket.Id)
		}
	}
	// 写入磁盘
	_, err := bm.fd.Write(bytes)
	return err
}

// IDGenerator ID生成器
type IDGenerator struct {
	// 当前最大ID
	currentMaxId uint64
}

// GenId 生成ID
// 当前最大ID+1，并返回新的当前最大ID
func (g *IDGenerator) GenId() uint64 {
	g.currentMaxId++
	return g.currentMaxId
}

// CompareAndSetMaxId 比较并设置最大ID
// 如果传入的ID比当前最大ID大，则将当前最大ID更新为传入ID
func (g *IDGenerator) CompareAndSetMaxId(id uint64) {
	if id > g.currentMaxId {
		g.currentMaxId = id
	}
}

// ExistBucket 桶是否存在
func (bm *BucketManager) ExistBucket(ds Ds, name BucketName) bool {
	// 获取桶
	bucket, err := bm.GetBucket(ds, name)
	// 如果桶实例不为nil，且err为nil则返回ture，否则返回false
	if bucket != nil && err == nil {
		return true
	}
	return false
}

// GetBucket 通过数据结构+表名获取桶
func (bm *BucketManager) GetBucket(ds Ds, name BucketName) (b *Bucket, err error) {
	// 先通过BucketIDMarker获取ds2IdMapper
	ds2IdMapper := bm.BucketIDMarker[name]
	// 再判断ds2IdMapper是否为nil，如果是则返回ErrBucketNotExist
	if ds2IdMapper == nil {
		return nil, ErrBucketNotExist
	}

	// 否则尝试从ds2IdMapper中获取桶ID，如果存在则进一步获取桶实例并返回
	// 如果不存在则返回ErrBucketNotExist
	if id, exist := ds2IdMapper[ds]; exist {
		if bucket, ok := bm.BucketInfoMapper[id]; ok {
			return bucket, nil
		} else {
			return nil, ErrBucketNotExist
		}
	} else {
		return nil, ErrBucketNotExist
	}
}

// GetBucketById 通过ID获取桶
func (bm *BucketManager) GetBucketById(id BucketId) (*Bucket, error) {
	// 通过BucketID获取桶实例，获取到了则返回桶实例，否则返回ErrBucketNotExist
	if bucket, exist := bm.BucketInfoMapper[id]; exist {
		return bucket, nil
	} else {
		return nil, ErrBucketNotExist
	}
}

// GetBucketID 通过数据结构+表名获取桶ID
func (bm *BucketManager) GetBucketID(ds Ds, name BucketName) (BucketId, error) {
	// 尝试获取桶，如果失败则返回0，否则返回对应的桶ID
	if bucket, err := bm.GetBucket(ds, name); err != nil {
		return 0, err
	} else {
		return bucket.Id, nil
	}
}
