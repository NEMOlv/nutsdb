// Copyright 2019 The nutsdb Author. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nutsdb

import (
	"errors"
	"math"
	"math/big"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/xujiajun/utils/strconv2"
)

const (
	getAllType    uint8 = 0
	getKeysType   uint8 = 1
	getValuesType uint8 = 2
)

// Put sets the value for a key in the bucket.
// a wrapper of the function put.
// Put 向桶中插入键值对数据。
func (tx *Tx) Put(bucket string, key, value []byte, ttl uint32) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().UnixMilli()), DataStructureBTree)
}

// PutWithTimestamp 向桶中插入键值对数据，但需要手动传入Timestamp
func (tx *Tx) PutWithTimestamp(bucket string, key, value []byte, ttl uint32, timestamp uint64) error {
	return tx.put(bucket, key, value, ttl, DataSetFlag, timestamp, DataStructureBTree)
}

// PutIfNotExists set the value for a key in the bucket only if the key doesn't exist already.
// PutIfNotExists 如果键不存在，向桶中插入键值对数据
func (tx *Tx) PutIfNotExists(bucket string, key, value []byte, ttl uint32) error {
	// 这里检查事务是否关闭，是因为内部使用到了tx的db实例
	// 如果tx已经关闭，那么db实例应该为空
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	// 获取桶
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return err
	}
	// 获取桶ID
	bucketId := b.Id

	// 如果桶不存在则返回找不到桶的错误
	// 实际上每个桶就是一个独立的内存索引
	idx, bucketExists := tx.db.Index.bTree.exist(bucketId)
	if !bucketExists {
		return ErrNotFoundBucket
	}
	// 从桶中获取record
	record, recordExists := idx.Find(key)
	// 如果记录存在，且没有过期，则返回空，也就是不进行插入
	if recordExists && !record.IsExpired() {
		return nil
	}

	return tx.put(bucket, key, value, ttl, DataSetFlag, uint64(time.Now().UnixMilli()), DataStructureBTree)
}

// PutIfExists set the value for a key in the bucket only if the key already exists.
// PutIfExists 仅在key已存在的情况下，才为数据桶中的键设置值。
func (tx *Tx) PutIfExists(bucket string, key, value []byte, ttl uint32) error {
	return tx.update(
		bucket,
		key,
		// 更新value的匿名函数
		func(_ []byte) ([]byte, error) { return value, nil },
		// 更新ttl的匿名函数
		func(_ uint32) (uint32, error) { return ttl, nil },
	)
}

// Get retrieves the value for a key in the bucket.
// The returned value is only valid for the life of the transaction.
// Get 检索桶中某个键的值
// 返回值仅在事务生命周期内有效。
func (tx *Tx) Get(bucket string, key []byte) (value []byte, err error) {
	return tx.get(bucket, key)
}

// get 检索桶中某个键的值
func (tx *Tx) get(bucket string, key []byte) (value []byte, err error) {
	// 检查事务是否关闭
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	// 获取桶
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil, err
	}
	bucketId := b.Id

	bucketStatus := tx.getBucketStatus(DataStructureBTree, bucket)
	if bucketStatus == BucketStatusDeleted {
		return nil, ErrBucketNotFound
	}

	// 如果当前事务是读写事务，则尝试从pendingWrite中获取数据
	status, entry := tx.findEntryAndItsStatus(DataStructureBTree, bucket, string(key))
	if status != NotFoundEntry && entry != nil {
		// 如果数据已被删除返回nil，否则返回value
		if status == EntryDeleted {
			return nil, ErrKeyNotFound
		} else {
			return entry.Value, nil
		}
	}

	// 获取内存索引（桶），如果内存索引（桶）不存在则返回nil,ErrNotFoundBucket
	idx, bucketExists := tx.db.Index.bTree.exist(bucketId)
	if !bucketExists {
		return nil, ErrNotFoundBucket
	}

	// 从内存索引（桶）中获取记录record
	record, found := idx.Find(key)
	// 如果找不到record则返回nil, ErrKeyNotFound
	if !found {
		return nil, ErrKeyNotFound
	}
	// 如果记录过期则调用putDeleteLog，返回nil, ErrNotFoundKey
	// 使用putDeleteLog有一个原因是，这些操作是在读事务中发生的，只管执行删除操作，不管成功与否。如果删除失败，下次读的时候再次删除即可。
	// 而如果是写事务，则不能使用该操作，因为该操作失败后无法返回错误
	if record.IsExpired() {
		tx.putDeleteLog(bucketId, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
		return nil, ErrNotFoundKey
	}
	// 从磁盘文件上读取key对应的value
	value, err = tx.db.getValueByRecord(record)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// ValueLen 返回值的大小
func (tx *Tx) ValueLen(bucket string, key []byte) (int, error) {
	value, err := tx.get(bucket, key)
	return len(value), err
}

// 获取最大键
func (tx *Tx) GetMaxKey(bucket string) ([]byte, error) {
	return tx.getMaxOrMinKey(bucket, true)
}

// 获取最小键
func (tx *Tx) GetMinKey(bucket string) ([]byte, error) {
	return tx.getMaxOrMinKey(bucket, false)
}

func (tx *Tx) getMaxOrMinKey(bucket string, isMax bool) ([]byte, error) {
	// 检查事务是否关闭
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}

	// 获取桶
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil, err
	}
	bucketId := b.Id

	// 检查桶是否存在
	idx, bucketExists := tx.db.Index.bTree.exist(bucketId)
	if !bucketExists {
		return nil, ErrNotFoundBucket
	}

	// 通过调用Max或Min函数获取桶中的最大key或最小key，如果不存在则返回nil, ErrKeyNotFound
	var (
		item  *Item
		found bool
	)

	if isMax {
		item, found = idx.Max()
	} else {
		item, found = idx.Min()
	}

	if !found {
		return nil, ErrKeyNotFound
	}

	// 如果记录过期了，则调用putDeleteLog，并且返回nil, ErrNotFoundKey
	if item.record.IsExpired() {
		tx.putDeleteLog(bucketId, item.key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
		return nil, ErrNotFoundKey
	}

	return item.key, nil
}

// GetAll returns all keys and values in the given bucket.
// GetAll 返回给定桶中的所有键和值
func (tx *Tx) GetAll(bucket string) ([][]byte, [][]byte, error) {
	return tx.getAllOrKeysOrValues(bucket, getAllType)
}

// GetKeys returns all keys in the given bucket.
// GetKeys 返回给定桶中的所有键
func (tx *Tx) GetKeys(bucket string) ([][]byte, error) {
	keys, _, err := tx.getAllOrKeysOrValues(bucket, getKeysType)
	return keys, err
}

// GetValues returns all values in the given bucket.
// GetValues 返回给定桶中的所有值
func (tx *Tx) GetValues(bucket string) ([][]byte, error) {
	_, values, err := tx.getAllOrKeysOrValues(bucket, getValuesType)
	return values, err
}

// getAllOrKeysOrValues 返回键或值
func (tx *Tx) getAllOrKeysOrValues(bucket string, typ uint8) ([][]byte, [][]byte, error) {
	// 检查事务是否关闭
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, nil, err
	}

	// 获取桶
	bucketId, err := tx.db.bm.GetBucketID(DataStructureBTree, bucket)
	if err != nil {
		return nil, nil, err
	}

	// 检查桶是否存在
	idx, bucketExists := tx.db.Index.bTree.exist(bucketId)
	if !bucketExists {
		return nil, nil, ErrNotFoundBucket
	}

	// 获取所有记录
	records := idx.All()

	var (
		keys   [][]byte
		values [][]byte
	)

	// 根据传入的查询类型，指定不同的参数
	switch typ {
	case getAllType:
		keys, values, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, bucketId, true, true)
	case getKeysType:
		keys, _, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, bucketId, true, false)
	case getValuesType:
		_, values, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, bucketId, false, true)
	}

	if err != nil {
		return nil, nil, err
	}

	return keys, values, nil
}

// GetSet
// todo 不知道在干啥
func (tx *Tx) GetSet(bucket string, key, value []byte) (oldValue []byte, err error) {
	err = tx.update(
		bucket,
		key,
		// 更新value匿名函数
		// 将旧value赋值给oldValue用于返回, 并传入新值value
		func(b []byte) ([]byte, error) {
			oldValue = b
			return value, nil
		},
		// 更新ttl匿名函数
		// 生命周期不改变
		func(oldTTL uint32) (uint32, error) {
			return oldTTL, nil
		},
	)

	return

}

// RangeScan query a range at given bucket, start and end slice.
// RangeScan 从指定的桶中查询start到end范围的数组
func (tx *Tx) RangeScan(bucket string, start, end []byte) (values [][]byte, err error) {
	// 检查事务是否关闭
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}
	// 获取桶
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil, err
	}
	bucketId := b.Id

	// 获取索引
	if index, ok := tx.db.Index.bTree.exist(bucketId); ok {
		// 从索引中获取记录
		records := index.Range(start, end)

		// 获取对应的值
		_, values, err = tx.getHintIdxDataItemsWrapper(records, ScanNoLimit, bucketId, false, true)
		if err != nil {
			return nil, ErrRangeScan
		}
	}

	if len(values) == 0 {
		return nil, ErrRangeScan
	}

	return
}

// PrefixScan iterates over a key prefix at given bucket, prefix and limitNum.
// LimitNum will limit the number of entries return.
// PrefixScan 从给定的桶、前缀和限制查询数量中遍历key前缀
// LimitNum 将限制返回条目的数量。
// offsetNum 将设定返回条目的起始位置
func (tx *Tx) PrefixScan(bucket string, prefix []byte, offsetNum int, limitNum int) (values [][]byte, err error) {
	// 检查事务是否关闭
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}
	// 获取桶
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil, err
	}
	bucketId := b.Id

	// 获取索引
	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		// 从索引中获取记录
		records := idx.PrefixScan(prefix, offsetNum, limitNum)
		// 获取值
		_, values, err = tx.getHintIdxDataItemsWrapper(records, limitNum, bucketId, false, true)
		if err != nil {
			return nil, ErrPrefixScan
		}
	}

	if len(values) == 0 {
		return nil, ErrPrefixScan
	}

	return
}

// PrefixSearchScan iterates over a key prefix at given bucket, prefix, match regular expression and limitNum.
// LimitNum will limit the number of entries return.
//
// PrefixSearchScan 根据给定的桶、前缀、正则表达式、限制返回数量遍历key前缀
// LimitNum 将限制返回条目的数量。
// offsetNum 将设定返回条目的起始位置
// match regular expression 满足正则表达式的key才会返回
func (tx *Tx) PrefixSearchScan(bucket string, prefix []byte, reg string, offsetNum int, limitNum int) (values [][]byte, err error) {
	// 检查事务是否关闭
	if err := tx.checkTxIsClosed(); err != nil {
		return nil, err
	}
	// 获取桶
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return nil, err
	}
	bucketId := b.Id

	// 获取索引
	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		// 获取记录
		records := idx.PrefixSearchScan(prefix, reg, offsetNum, limitNum)
		// 获取值
		_, values, err = tx.getHintIdxDataItemsWrapper(records, limitNum, bucketId, false, true)
		if err != nil {
			return nil, ErrPrefixSearchScan
		}
	}

	if len(values) == 0 {
		return nil, ErrPrefixSearchScan
	}

	return
}

// Delete removes a key from the bucket at given bucket and key.
// Delete 根据给定的桶和key从桶中删除相关记录
func (tx *Tx) Delete(bucket string, key []byte) error {
	// 检查事务是否关闭
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	// 获取桶
	b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
	if err != nil {
		return err
	}
	bucketId := b.Id

	// 获取索引，获取不到返回ErrNotFoundBucket
	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		// 找到指定的key，如果找不到则返回ErrKeyNotFound
		if _, found := idx.Find(key); !found {
			return ErrKeyNotFound
		}
	} else {
		return ErrNotFoundBucket
	}
	// 添加一条删除数据
	// 为什么这里不用putDeleteLog，因为这是一个写事务，要处理删除失败的情况
	return tx.put(bucket, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
}

// getHintIdxDataItemsWrapper returns keys and values when prefix scanning or range scanning.
// getHintIdxDataItemsWrapper 当使用前缀扫描或范围扫描时反馈key或value
func (tx *Tx) getHintIdxDataItemsWrapper(records []*Record, limitNum int, bucketId BucketId, needKeys bool, needValues bool) (keys [][]byte, values [][]byte, err error) {
	// 循环获取每一个record
	for _, record := range records {
		// 如果记录过期则删除并继续下一次循环
		if record.IsExpired() {
			tx.putDeleteLog(bucketId, record.Key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
			continue
		}

		// 如果查询数量限制>0并且value数组的长度小于查询数量限制 或 查询数量限制为-1,则选择是否加入数组
		if limitNum > 0 && len(values) < limitNum || limitNum == ScanNoLimit {
			// 如果需要key，则添加进keys数组
			if needKeys {
				keys = append(keys, record.Key)
			}

			// 如果需要value，则添加进values数组
			if needValues {
				value, err := tx.db.getValueByRecord(record)
				if err != nil {
					return nil, nil, err
				}
				values = append(values, value)
			}
		}
	}

	return keys, values, nil
}

// tryGet
func (tx *Tx) tryGet(bucket string, key []byte, solveRecord func(record *Record, found bool, bucketId BucketId) error) error {
	// 检查事务是否关闭
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	// 获取桶
	bucketId, err := tx.db.bm.GetBucketID(DataStructureBTree, bucket)
	if err != nil {
		return err
	}

	// 获取索引
	// 如果索引存在则调用solveRecord函数处理record，否则返回ErrBucketNotFound
	if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
		record, found := idx.Find(key)
		return solveRecord(record, found, bucketId)
	} else {
		return ErrBucketNotFound
	}
}

// update
func (tx *Tx) update(bucket string, key []byte, getNewValue func([]byte) ([]byte, error), getNewTTL func(uint32) (uint32, error)) error {
	return tx.tryGet(bucket, key, func(record *Record, found bool, bucketId BucketId) error {
		// 如果record找不到则返回ErrKeyNotFound
		if !found {
			return ErrKeyNotFound
		}

		// 如果record过期则删除记录，并返回ErrKeyNotFound
		if record.IsExpired() {
			tx.putDeleteLog(bucketId, key, nil, Persistent, DataDeleteFlag, uint64(time.Now().Unix()), DataStructureBTree)
			return ErrNotFoundKey
		}

		// 从record中获取value
		value, err := tx.db.getValueByRecord(record)
		if err != nil {
			return err
		}

		// 传入旧值, 获取新值
		newValue, err := getNewValue(value)
		if err != nil {
			return err
		}

		// 传入旧生命周期，获取新生命周期
		newTTL, err := getNewTTL(record.TTL)
		if err != nil {
			return err
		}
		// 更新数据
		return tx.put(bucket, key, newValue, newTTL, DataSetFlag, uint64(time.Now().Unix()), DataStructureBTree)
	})
}

// updateOrPut 更新或添加数据
func (tx *Tx) updateOrPut(bucket string, key, value []byte, getUpdatedValue func([]byte) ([]byte, error)) error {
	return tx.tryGet(
		bucket,
		key,
		func(record *Record, found bool, bucketId BucketId) error {
			// 如果record不存在则添加数据
			if !found {
				return tx.put(bucket, key, value, Persistent, DataSetFlag, uint64(time.Now().Unix()), DataStructureBTree)
			}

			// 如果存在则更新
			value, err := tx.db.getValueByRecord(record)
			if err != nil {
				return err
			}

			// 传入旧值，获取新值
			newValue, err := getUpdatedValue(value)
			if err != nil {
				return err
			}
			// 更新数据
			return tx.put(bucket, key, newValue, record.TTL, DataSetFlag, uint64(time.Now().Unix()), DataStructureBTree)
		},
	)
}

// bigIntIncr 大数相加
func bigIntIncr(a string, b string) string {
	bigIntA, _ := new(big.Int).SetString(a, 10)
	bigIntB, _ := new(big.Int).SetString(b, 10)
	bigIntA.Add(bigIntA, bigIntB)
	return bigIntA.String()
}

// integerIncr
func (tx *Tx) integerIncr(bucket string, key []byte, increment int64) error {
	return tx.update(
		bucket,
		key,
		// 更新value
		func(value []byte) ([]byte, error) {
			intValue, err := strconv2.StrToInt64(string(value))

			// 如果err不为nil 且 err是strconv.ErrRange错，则返回
			if err != nil && errors.Is(err, strconv.ErrRange) {
				// 大数相加后转为字节数组返回
				return []byte(bigIntIncr(string(value), strconv2.Int64ToStr(increment))), nil
			}

			// 如果err不为nil，返回ErrValueNotInteger
			if err != nil {
				return nil, ErrValueNotInteger
			}

			// 如果 (increment大于0 且 64位最大值-increment小于当前intValue) 或 (increment小于0 且 64位最小值-increment大于当前intValue)
			// 也就是如果 intValue+increment > MaxInt64 或 intValue+increment < MinInt64超过整数相加，则改用大数相加
			if (increment > 0 && math.MaxInt64-increment < intValue) || (increment < 0 && math.MinInt64-increment > intValue) {
				// 大数相加后转为字节数组返回
				return []byte(bigIntIncr(string(value), strconv2.Int64ToStr(increment))), nil
			}

			// 正常原子性正数相加，并返回字节数组
			atomic.AddInt64(&intValue, increment)
			return []byte(strconv2.Int64ToStr(intValue)), nil
		},
		// 不更新TTL
		func(oldTTL uint32) (uint32, error) {
			return oldTTL, nil
		},
	)
}

// Incr value+1
func (tx *Tx) Incr(bucket string, key []byte) error {
	return tx.integerIncr(bucket, key, 1)
}

// Decr value-1
func (tx *Tx) Decr(bucket string, key []byte) error {
	return tx.integerIncr(bucket, key, -1)
}

// IncrBy value+increment
func (tx *Tx) IncrBy(bucket string, key []byte, increment int64) error {
	return tx.integerIncr(bucket, key, increment)
}

// DecrBy value-decrement
func (tx *Tx) DecrBy(bucket string, key []byte, decrement int64) error {
	return tx.integerIncr(bucket, key, -1*decrement)
}

// GetBit 返回value中下标在offset的数据
// todo 这是什么奇葩功能？
func (tx *Tx) GetBit(bucket string, key []byte, offset int) (byte, error) {
	// 如果offset > MaxInt 或 offset < 0，返回ErrOffsetInvalid
	if offset >= math.MaxInt || offset < 0 {
		return 0, ErrOffsetInvalid
	}

	// 获取value
	value, err := tx.Get(bucket, key)
	if err != nil {
		return 0, err
	}

	// 如果value的长度小于等于offset，返回nil
	if len(value) <= offset {
		return 0, nil
	}

	// 返回value在下标为offset上的数据
	return value[offset], nil
}

// SetBit 设置value中下标在offset的数据
func (tx *Tx) SetBit(bucket string, key []byte, offset int, bit byte) error {
	// 如果offset > MaxInt 或 offset < 0，返回ErrOffsetInvalid
	if offset >= math.MaxInt || offset < 0 {
		return ErrOffsetInvalid
	}

	// 如果key没找到，则创建一个新的value数组
	valueIfKeyNotFound := make([]byte, offset+1)
	valueIfKeyNotFound[offset] = bit

	// 更新或添加数据
	return tx.updateOrPut(
		bucket,
		key,
		valueIfKeyNotFound,
		func(value []byte) ([]byte, error) {
			// 如果value的长度小于offset，则扩展value的长度，并将bit数据添加到下标为offset的位置上
			// 否则直接将bit数据添加到下标为offset的位置上
			if len(value) <= offset {
				value = append(value, make([]byte, offset-len(value)+1)...)
				value[offset] = bit
				return value, nil
			} else {
				value[offset] = bit
				return value, nil
			}
		},
	)
}

// GetTTL returns remaining TTL of a value by key.
// It returns
// (-1, nil) If TTL is Persistent
// (0, ErrBucketNotFound|ErrKeyNotFound) If expired or not found
// (TTL, nil) If the record exists with a TTL
// Note: The returned remaining TTL will be in seconds. For example,
// remainingTTL is 500ms, It'll return 0.
//
// GetTTL 返回记录的剩余TTL
// 返回(-1, nil)代表：该记录是Persistent的
// 返回(0, ErrBucketNotFound|ErrKeyNotFound) 代表：记录过期或没有找到
// 返回(TTL, nil)代表记录存在且携带TTL
// 返回的剩余TTL是秒级的，如果剩余TTL只用500ms，那么回返回0s
func (tx *Tx) GetTTL(bucket string, key []byte) (int64, error) {
	// 检查事务是否关闭
	if err := tx.checkTxIsClosed(); err != nil {
		return 0, err
	}

	// 获取桶
	bucketId, err := tx.db.bm.GetBucketID(DataStructureBTree, bucket)
	if err != nil {
		return 0, err
	}
	idx, bucketExists := tx.db.Index.bTree.exist(bucketId)
	if !bucketExists {
		return 0, ErrBucketNotFound
	}

	// 获取记录
	record, recordFound := idx.Find(key)
	if !recordFound || record.IsExpired() {
		return 0, ErrKeyNotFound
	}

	// 如果TTL=Persistent，代表该记录没有TTL
	if record.TTL == Persistent {
		return -1, nil
	}

	// 如果remTTL>0返回剩余的TTL，否则返回ErrKeyNotFound
	remTTL := tx.db.expireTime(record.Timestamp, record.TTL)
	if remTTL >= 0 {
		return int64(remTTL.Seconds()), nil
	} else {
		return 0, ErrKeyNotFound
	}
}

// Persist updates record's TTL as Persistent if the record exits.
// Persist 将记录的TTL更新为TTL
func (tx *Tx) Persist(bucket string, key []byte) error {
	return tx.update(
		bucket,
		key,
		// 不更新value
		func(oldValue []byte) ([]byte, error) {
			return oldValue, nil
		},
		// 将TTL更新为Persistent
		func(_ uint32) (uint32, error) {
			return Persistent, nil
		},
	)
}

// MSet 循环插入多条数据
func (tx *Tx) MSet(bucket string, ttl uint32, args ...[]byte) error {
	// 如果args为0，返回nil
	if len(args) == 0 {
		return nil
	}

	// 如果len(args)不是偶数返回，ErrKVArgsLenNotEven
	if len(args)%2 != 0 {
		return ErrKVArgsLenNotEven
	}

	// 循环插入len(args)/2条KV数据
	for i := 0; i < len(args); i += 2 {
		if err := tx.put(bucket, args[i], args[i+1], ttl, DataSetFlag, uint64(time.Now().Unix()), DataStructureBTree); err != nil {
			return err
		}
	}

	return nil
}

// MGet 循环获取多条数据
func (tx *Tx) MGet(bucket string, keys ...[]byte) ([][]byte, error) {
	// 如果keys为0，返回nil
	if len(keys) == 0 {
		return nil, nil
	}

	// 创建一个长度为len(keys)的二维数组values
	// 一开就创建好足够的空间，就不会因为append而导致扩容
	values := make([][]byte, len(keys))
	// 循环获取value，并添加进values数组
	for i, key := range keys {
		value, err := tx.Get(bucket, key)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}

	return values, nil
}

// Append 追加数据
// 如果不存在key对应的记录，则直接插入一条新记录
// 如果存在key对应的记录，则先获取对应的value，然后在value的尾部添加appendage
func (tx *Tx) Append(bucket string, key, appendage []byte) error {
	// 如果len(appendage)=0，则返回nil
	if len(appendage) == 0 {
		return nil
	}

	// 更新或添加数据
	return tx.updateOrPut(
		bucket,
		key,
		appendage,
		// 更新value
		func(value []byte) ([]byte, error) {
			return append(value, appendage...), nil
		},
	)
}

// GetRange 获取value的一部分，从start到end
func (tx *Tx) GetRange(bucket string, key []byte, start, end int) ([]byte, error) {
	if start > end {
		return nil, ErrStartGreaterThanEnd
	}

	value, err := tx.get(bucket, key)
	if err != nil {
		return nil, err
	}

	if start >= len(value) {
		return nil, nil
	}

	if end >= len(value) {
		return value[start:], nil
	}

	return value[start : end+1], nil
}
