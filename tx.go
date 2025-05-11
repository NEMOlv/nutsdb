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
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/bwmarrin/snowflake"
	"github.com/xujiajun/utils/strconv2"
)

const (
	// txStatusRunning means the tx is running
	// 代表事务正在执行
	txStatusRunning = 1
	// txStatusCommitting means the tx is committing
	// 代表事务正在提交
	txStatusCommitting = 2
	// txStatusClosed means the tx is closed, ether committed or rollback
	// 代表事务已关闭，可能是提交结束也可能是回滚
	txStatusClosed = 3
)

// Tx represents a transaction.
// Tx 代表事务
type Tx struct {
	// 文件ID
	id uint64
	// 数据库实例
	db *DB
	// 是否写事务
	writable bool
	// 事务状态
	status atomic.Value
	// 待写键值对数组
	pendingWrites *pendingEntryList
	// 本次事务的数据大小
	size int64
	// 待写Bucket数组
	pendingBucketList pendingBucketList
}

// txnCb 事务回调结构体
//
//	txn: Transaction
//	Cb:	 Callback
type txnCb struct {
	// 提交事务
	commit func() error
	// 用于在事务完成后执行用户自定义的逻辑
	user func(error)
	// 错误
	err error
}

// submitEntry 提交键值对
// 入参：
//
//	ds：数据结构类型
//	bucket：桶名
//	e：键值对
func (tx *Tx) submitEntry(ds uint16, bucket string, e *Entry) {
	// 将键值对写入待写数组
	tx.pendingWrites.submitEntry(ds, bucket, e)
}

// runTxnCallback: 执行事务回调
func runTxnCallback(cb *txnCb) {
	switch {
	// 如果事务回调结构体为空，则返回
	case cb == nil:
		panic("tx callback is nil")
	//	如果事务回调结构体中的user自定义函数为空，一定是捕捉到了 tx.CommitWith 的回调为空
	case cb.user == nil:
		panic("Must have caught a nil callback for tx.CommitWith")
	//	如果事务回调结构体中的err不为空，则使用自定义user函数处理该err
	case cb.err != nil:
		cb.user(cb.err)
	//	如果事务回调结构体中的commit不为空，执行提交，如果有错误，则使用自定义user函数处理该err
	case cb.commit != nil:
		err := cb.commit()
		cb.user(err)
	//	默认情况下，传入一个nil值给自定义user函数
	default:
		cb.user(nil)
	}
}

// Begin opens a new transaction.
// Multiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
// All transactions must be closed by calling Commit() or Rollback() when done.
// Begin 打开一个新事务
// 只读事务可以同时打开多个，读写事务同时只能打开一个
// 当尝试打开一个读写事务时，其他进程中的事务将会被阻塞，直到当前读写事务完成
// 所有事务结束时必须通过调用提交函数或回滚函数关闭
func (db *DB) Begin(writable bool) (tx *Tx, err error) {
	// 打开新事务
	tx, err = newTx(db, writable)
	if err != nil {
		return nil, err
	}

	// 事务获取锁
	tx.lock()
	// 事务设置为执行状态
	tx.setStatusRunning()
	// 如果数据库关闭了
	if db.closed {
		// 事务释放锁
		tx.unlock()
		// 事务设置为关闭状态
		tx.setStatusClosed()
		// 返回nil,事务关闭错误
		return nil, ErrDBClosed
	}

	// 返回事务Tx,nil
	return
}

// newTx returns a newly initialized Tx object at given writable.
// newTx 返回一个新初始化的事务Tx实例
func newTx(db *DB, writable bool) (tx *Tx, err error) {
	// 声明事务ID
	var txID uint64

	// 创建事务
	tx = &Tx{
		// 数据库实例
		db: db,
		// 是否可写
		writable: writable,
		// 待写键值对数组
		pendingWrites: newPendingEntriesList(),
		// 待写bucket数组
		// 		第一个map: 数据结构类型：该类型的桶map
		// 		第二个map：桶名：桶实例
		pendingBucketList: make(map[Ds]map[BucketName]*Bucket),
	}

	// 生成事务ID
	txID, err = tx.getTxID()
	if err != nil {
		return nil, err
	}

	// 将生成的事务ID赋值给当前事务
	tx.id = txID

	return
}

func (tx *Tx) CommitWith(cb func(error)) {
	if cb == nil {
		panic("Nil callback provided to CommitWith")
	}

	if tx.pendingWrites.size == 0 {
		// Do not run these callbacks from here, because the CommitWith and the
		// callback might be acquiring the same locks. Instead run the callback
		// from another goroutine.
		go runTxnCallback(&txnCb{user: cb, err: nil})
		return
	}
	// defer tx.setStatusClosed()  //must not add this code because another process is also accessing tx
	commitCb, err := tx.commitAndSend()
	if err != nil {
		go runTxnCallback(&txnCb{user: cb, err: err})
		return
	}

	go runTxnCallback(&txnCb{user: cb, commit: commitCb})
}

func (tx *Tx) commitAndSend() (func() error, error) {
	req, err := tx.db.sendToWriteCh(tx)
	if err != nil {
		return nil, err
	}
	ret := func() error {
		err := req.Wait()
		return err
	}

	return ret, nil
}

func (tx *Tx) checkSize() error {
	count := tx.pendingWrites.size
	if int64(count) >= tx.db.getMaxBatchCount() || tx.size >= tx.db.getMaxBatchSize() {
		return ErrTxnTooBig
	}

	return nil
}

// getTxID returns the tx id.
func (tx *Tx) getTxID() (id uint64, err error) {
	// 根据配置项的节点数量创建雪花节点，生成并返回ID
	node, err := snowflake.NewNode(tx.db.opt.NodeNum)
	if err != nil {
		return 0, err
	}

	id = uint64(node.Generate().Int64())

	return
}

// Commit commits the transaction, following these steps:
//
// 1. check the length of pendingWrites.If there are no writes, return immediately.
//
// 2. check if the ActiveFile has not enough space to store entry. if not, call rotateActiveFile function.
//
// 3. write pendingWrites to disk, if a non-nil error,return the error.
//
// 4. build Hint index.
//
// 5. Unlock the database and clear the db field.
//
// Commit 根据以下步骤提交事务：
// 1.检查待写数组长度，如果为空立即返回
// 2.检查活跃文件是否有充足的空间存储数据，如果不足则调用rotateActiveFile函数
// 3.将待写数组写入磁盘，如果错误不为空，则返回错误
// 4.构建Hint索引
// 5.释放数据库的锁并清理db字段
func (tx *Tx) Commit() (err error) {
	// 后处理
	defer func() {
		if err != nil {
			tx.handleErr(err)
		}
		tx.unlock()
		tx.db = nil
		tx.pendingWrites = nil
	}()

	// 如果事务已经关闭，返回无法提交已关闭事务的错误
	if tx.isClosed() {
		return ErrCannotCommitAClosedTx
	}

	// 如果数据库实例是nil
	// 关闭当前事务，并返回数据库关闭的错误
	if tx.db == nil {
		tx.setStatusClosed()
		return ErrDBClosed
	}

	// 声明当前写入数量
	var curWriteCount int64
	// 如果配置项的最大写入数量大于0
	if tx.db.opt.MaxWriteRecordCount > 0 {
		// 获取当前写入数量
		curWriteCount, err = tx.getNewAddRecordCount()
		if err != nil {
			return err
		}

		// judge all write records is whether more than the MaxWriteRecordCount
		// 将所有写入数量与最大写入数量做比较
		// 如果超过最大写入数量，则返回超过最大写入数量的错误
		if tx.db.RecordCount+curWriteCount > tx.db.opt.MaxWriteRecordCount {
			return ErrTxnExceedWriteLimit
		}
	}

	// 将事务状态设置为提交
	tx.setStatusCommitting()
	// 并在执行结束后修改为关闭
	defer tx.setStatusClosed()

	writesBucketLen := len(tx.pendingBucketList)
	if tx.pendingWrites.size == 0 && writesBucketLen == 0 {
		return nil
	}

	// 分配提交Buffer
	buff := tx.allocCommitBuffer()
	// 清理buffer
	defer tx.db.commitBuffer.Reset()

	// 声明记录数组
	var records []*Record
	// 从待写结构体中获取待写数组
	pendingWriteList := tx.pendingWrites.toList()
	lastIndex := len(pendingWriteList) - 1
	for i := 0; i < len(pendingWriteList); i++ {
		entry := pendingWriteList[i]
		entrySize := entry.Size()
		// 如果键值对的大小大于段文件大小，返回数据大小过大的错误
		if entrySize > tx.db.opt.SegmentSize {
			return ErrDataSizeExceed
		}
		// 如果累计写入的数据大小超过段文件大小，则将之前buffer中的数据写入活跃文件，然后打开新的活跃文件
		if tx.db.ActiveFile.ActualSize+int64(buff.Len())+entrySize > tx.db.opt.SegmentSize {
			if _, err := tx.writeData(buff.Bytes()); err != nil {
				return err
			}
			buff.Reset()

			if err := tx.rotateActiveFile(); err != nil {
				return err
			}
		}

		// 当前写入偏移 = 活跃文件写入偏移 + buffer长度
		offset := tx.db.ActiveFile.writeOff + int64(buff.Len())

		// 如果是最后一条键值对，则将其状态修改为提交完成
		if i == lastIndex {
			entry.Meta.Status = Committed
		}

		// 将键值对编码并写入buffer
		if _, err := buff.Write(entry.Encode()); err != nil {
			return err
		}

		// 如果是最后一条键值对，将buffer写入磁盘
		if i == lastIndex {
			if _, err := tx.writeData(buff.Bytes()); err != nil {
				return err
			}
		}

		// 携带文件ID和写入偏移通过Mode创建记录
		record := tx.db.createRecordByModeWithFidAndOff(tx.db.ActiveFile.fileID, uint64(offset), entry)

		// add to cache
		// 将记录加入缓存
		if tx.db.getHintKeyAndRAMIdxCacheSize() > 0 && tx.db.opt.EntryIdxMode == HintKeyAndRAMIdxMode {
			tx.db.hintKeyAndRAMIdxModeLru.Add(record, entry)
		}

		// 将记录添加进记录数组
		records = append(records, record)
	}

	// 提交bucket
	if err := tx.SubmitBucket(); err != nil {
		return err
	}

	// 构造键值对索引
	if err := tx.buildIdxes(records, pendingWriteList); err != nil {
		return err
	}
	tx.db.RecordCount += curWriteCount

	// 构造bucket索引
	if err := tx.buildBucketInIndex(); err != nil {
		return err
	}

	return nil
}

// getNewAddRecordCount todo 不知道这里在做什么
// 获取新加入记录数量和桶的数量
func (tx *Tx) getNewAddRecordCount() (int64, error) {
	var res int64
	changeCountInEntries, err := tx.getChangeCountInEntriesChanges()
	changeCountInBucket := tx.getChangeCountInBucketChanges()
	res += changeCountInEntries
	res += changeCountInBucket
	return res, err
}

// getListHeadTailSeq
func (tx *Tx) getListHeadTailSeq(bucketId BucketId, key string) *HeadTailSeq {
	res := HeadTailSeq{Head: initialListSeq, Tail: initialListSeq + 1}
	if _, ok := tx.db.Index.list.idx[bucketId]; ok {
		if _, ok := tx.db.Index.list.idx[bucketId].Seq[key]; ok {
			res = *tx.db.Index.list.idx[bucketId].Seq[key]
		}
	}

	return &res
}

// getListEntryNewAddRecordCount
// 获取新加入的List记录数量
func (tx *Tx) getListEntryNewAddRecordCount(bucketId BucketId, entry *Entry) (int64, error) {
	if entry.Meta.Flag == DataExpireListFlag {
		return 0, nil
	}

	var res int64
	key := string(entry.Key)
	value := string(entry.Value)
	l := tx.db.Index.list.getWithDefault(bucketId)

	switch entry.Meta.Flag {
	case DataLPushFlag, DataRPushFlag:
		res++
	case DataLPopFlag, DataRPopFlag:
		res--
	case DataLRemByIndex:
		indexes, _ := UnmarshalInts([]byte(value))
		res -= int64(len(l.getValidIndexes(key, indexes)))
	case DataLRemFlag:
		count, newValue := splitIntStringStr(value, SeparatorForListKey)
		removeIndices, err := l.getRemoveIndexes(key, count, func(r *Record) (bool, error) {
			v, err := tx.db.getValueByRecord(r)
			if err != nil {
				return false, err
			}
			return bytes.Equal([]byte(newValue), v), nil
		})
		if err != nil {
			return 0, err
		}
		res -= int64(len(removeIndices))
	case DataLTrimFlag:
		newKey, start := splitStringIntStr(key, SeparatorForListKey)
		end, _ := strconv2.StrToInt(value)

		if l.IsExpire(newKey) {
			return 0, nil
		}

		if _, ok := l.Items[newKey]; !ok {
			return 0, nil
		}

		items, err := l.LRange(newKey, start, end)
		if err != nil {
			return res, err
		}

		list := l.Items[newKey]
		res -= int64(list.Count() - len(items))
	}

	return res, nil
}

// getKvEntryNewAddRecordCount
// 获取新加入的键值对记录数量
func (tx *Tx) getKvEntryNewAddRecordCount(bucketId BucketId, entry *Entry) (int64, error) {
	var res int64

	switch entry.Meta.Flag {
	case DataDeleteFlag:
		res--
	case DataSetFlag:
		if idx, ok := tx.db.Index.bTree.exist(bucketId); ok {
			_, found := idx.Find(entry.Key)
			if !found {
				res++
			}
		} else {
			res++
		}
	}

	return res, nil
}

// getSetEntryNewAddRecordCount
// 获取新加入的Set记录数量
func (tx *Tx) getSetEntryNewAddRecordCount(bucketId BucketId, entry *Entry) (int64, error) {
	var res int64

	if entry.Meta.Flag == DataDeleteFlag {
		res--
	}

	if entry.Meta.Flag == DataSetFlag {
		res++
	}

	return res, nil
}

// getSortedSetEntryNewAddRecordCount
// 获取新加入的SortedSet记录数量
func (tx *Tx) getSortedSetEntryNewAddRecordCount(bucketId BucketId, entry *Entry) (int64, error) {
	var res int64
	key := string(entry.Key)
	value := string(entry.Value)

	switch entry.Meta.Flag {
	case DataZAddFlag:
		if !tx.keyExistsInSortedSet(bucketId, key, value) {
			res++
		}
	case DataZRemFlag:
		res--
	case DataZRemRangeByRankFlag:
		start, end := splitIntIntStr(value, SeparatorForZSetKey)
		delNodes, err := tx.db.Index.sortedSet.getWithDefault(bucketId, tx.db).getZRemRangeByRankNodes(key, start, end)
		if err != nil {
			return res, err
		}
		res -= int64(len(delNodes))
	case DataZPopMaxFlag, DataZPopMinFlag:
		res--
	}

	return res, nil
}

// keyExistsInSortedSet 在SortedSet中是否存在当前key
func (tx *Tx) keyExistsInSortedSet(bucketId BucketId, key, value string) bool {
	if _, exist := tx.db.Index.sortedSet.exist(bucketId); !exist {
		return false
	}
	newKey := key
	if strings.Contains(key, SeparatorForZSetKey) {
		newKey, _ = splitStringFloat64Str(key, SeparatorForZSetKey)
	}
	exists, _ := tx.db.Index.sortedSet.idx[bucketId].ZExist(newKey, []byte(value))
	return exists
}

// getEntryNewAddRecordCount 获取新加入记录的数量
func (tx *Tx) getEntryNewAddRecordCount(entry *Entry) (int64, error) {
	var res int64
	var err error

	bucket, err := tx.db.bm.GetBucketById(entry.Meta.BucketId)
	if err != nil {
		return 0, err
	}
	bucketId := bucket.Id

	if entry.Meta.Ds == DataStructureBTree {
		res, err = tx.getKvEntryNewAddRecordCount(bucketId, entry)
	}

	if entry.Meta.Ds == DataStructureList {
		res, err = tx.getListEntryNewAddRecordCount(bucketId, entry)
	}

	if entry.Meta.Ds == DataStructureSet {
		res, err = tx.getSetEntryNewAddRecordCount(bucketId, entry)
	}

	if entry.Meta.Ds == DataStructureSortedSet {
		res, err = tx.getSortedSetEntryNewAddRecordCount(bucketId, entry)
	}

	return res, err
}

// allocCommitBuffer 分配提交buffer
// todo 没有使用缓存池，会不会导致频繁分配内存呢？
func (tx *Tx) allocCommitBuffer() *bytes.Buffer {
	// 声明buffer指针
	// 声明指针时不会立即分配内存空间，只有在为指针赋值时，才会分配内存空间
	var buff *bytes.Buffer
	// 如果事务大小小于配置项中的提交buffer大小，则将buff指向tx.db.commitBuffer的内存空间
	// 否则buff指向一个新分配的内存空间，并调用Grow函数，将内存空间增长到事务大小
	if tx.size < tx.db.opt.CommitBufferSize {
		buff = tx.db.commitBuffer
	} else {
		buff = new(bytes.Buffer)
		// avoid grow
		// 避免增长
		buff.Grow(int(tx.size))
	}

	return buff
}

// rotateActiveFile rotates log file when active file is not enough space to store the entry.
// rotateActiveFile 当活跃文件没有足够的空间存储数据时，更换活跃文件
func (tx *Tx) rotateActiveFile() error {
	var err error
	// 最大文件数+1
	tx.db.MaxFileID++

	// 如果配置项没开启同步并且读写模式为MMap
	if !tx.db.opt.SyncEnable && tx.db.opt.RWMode == MMap {
		// 同步活跃文件
		if err := tx.db.ActiveFile.rwManager.Sync(); err != nil {
			return err
		}
	}

	// 释放活跃文件
	if err := tx.db.ActiveFile.rwManager.Release(); err != nil {
		return err
	}

	// reset ActiveFile
	// 重置活跃文件
	path := getDataPath(tx.db.MaxFileID, tx.db.opt.Dir)
	tx.db.ActiveFile, err = tx.db.fm.getDataFile(path, tx.db.opt.SegmentSize)
	if err != nil {
		return err
	}

	tx.db.ActiveFile.fileID = tx.db.MaxFileID
	return nil
}

// writeData 写入数据
func (tx *Tx) writeData(data []byte) (n int, err error) {
	// 如果写入数据为空立即返回
	if len(data) == 0 {
		return
	}
	// 当前写入偏移是活跃文件的实际大小
	writeOffset := tx.db.ActiveFile.ActualSize

	// 获取数据长度
	l := len(data)
	// 如果当前写入偏移+数据长度>配置项的段文件大小，返回0和空间不足的错误
	if writeOffset+int64(l) > tx.db.opt.SegmentSize {
		return 0, errors.New("not enough file space")
	}

	// 将数据写入活跃文件
	if n, err = tx.db.ActiveFile.WriteAt(data, writeOffset); err != nil {
		return
	}

	// 更新写入偏移
	tx.db.ActiveFile.writeOff += int64(l)
	// 更新实际大小
	tx.db.ActiveFile.ActualSize += int64(l)

	// 如果开启了同步，则同步活跃文件
	if tx.db.opt.SyncEnable {
		if err := tx.db.ActiveFile.rwManager.Sync(); err != nil {
			return 0, err
		}
	}

	return
}

// Rollback closes the transaction.
// Rollback 关闭事务
// todo 你这算什么回滚？
func (tx *Tx) Rollback() error {
	// 如果数据库实例为空，则返回数据库关闭的错误
	if tx.db == nil {
		tx.setStatusClosed()
		return ErrDBClosed
	}

	// 如果事务处于提交状态，则返回无法回滚提交中的事务
	if tx.isCommitting() {
		return ErrCannotRollbackACommittingTx
	}

	// 如果事务处于关闭状态，则返回无法回滚已关闭的事务
	if tx.isClosed() {
		return ErrCannotRollbackAClosedTx
	}

	// 将事务状态设置为关闭
	tx.setStatusClosed()
	// 事务释放锁
	tx.unlock()

	// 事务拥有的数据库实例置空
	tx.db = nil
	// 事务的待写数组置空
	tx.pendingWrites = nil

	// 返回nil
	return nil
}

// lock locks the database based on the transaction type.
// lock 基于事务的不同状态对数据库加锁
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
// unlock 基于事务的不同状态对数据库解锁
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

// handleErr处理错误
func (tx *Tx) handleErr(err error) {
	if tx.db.opt.ErrorHandler != nil {
		tx.db.opt.ErrorHandler.HandleError(err)
	}
}

// checkTxIsClosed 检查事务是否关闭
func (tx *Tx) checkTxIsClosed() error {
	if tx.db == nil {
		return ErrTxClosed
	}
	return nil
}

// put sets the value for a key in the bucket.
// Returns an error if tx is closed, if performing a write operation on a read-only transaction, if the key is empty.
// put 将一个键值对写入桶中
// 如果 tx 已关闭，如果对只读事务执行写操作，如果键为空，则返回错误信息。
func (tx *Tx) put(bucket string, key, value []byte, ttl uint32, flag uint16, timestamp uint64, ds uint16) error {
	// 检查事务是否关闭
	if err := tx.checkTxIsClosed(); err != nil {
		return err
	}

	// 获取桶状态
	bucketStatus := tx.getBucketStatus(DataStructureBTree, bucket)
	// 如果桶已被删除，返回桶找不到的错误
	if bucketStatus == BucketStatusDeleted {
		return ErrBucketNotFound
	}
	// 从桶管理器中判断桶是否存在,如果不存在则返回桶不存在的错误
	if !tx.db.bm.ExistBucket(ds, bucket) {
		return ErrorBucketNotExist
	}
	// 如果事务不是写状态,返回不是写事务的错误
	if !tx.writable {
		return ErrTxNotWritable
	}

	// 从桶管理器中获取桶ID
	bucketId, err := tx.db.bm.GetBucketID(ds, bucket)
	if err != nil {
		return err
	}

	// 构造元数据
	meta := NewMetaData().WithTimeStamp(timestamp).WithKeySize(uint32(len(key))).WithValueSize(uint32(len(value))).WithFlag(flag).
		WithTTL(ttl).WithStatus(UnCommitted).WithDs(ds).WithTxID(tx.id).WithBucketId(bucketId)

	// 构造键值对
	e := NewEntry().WithKey(key).WithMeta(meta).WithValue(value)

	// 检查键值对是否有效
	err = e.valid()
	if err != nil {
		return err
	}

	// 提交键值对
	tx.submitEntry(ds, bucket, e)
	// 增加事务大小
	tx.size += e.Size()

	return nil
}

// putDeleteLog 将数据写入删除日志
// todo 暂时没看出和一般的写入有什么区别
func (tx *Tx) putDeleteLog(bucketId BucketId, key, value []byte, ttl uint32, flag uint16, timestamp uint64, ds uint16) {
	bucket, err := tx.db.bm.GetBucketById(bucketId)
	if err != nil {
		return
	}
	meta := NewMetaData().WithTimeStamp(timestamp).WithKeySize(uint32(len(key))).WithValueSize(uint32(len(value))).WithFlag(flag).
		WithTTL(ttl).WithStatus(UnCommitted).WithDs(ds).WithTxID(tx.id).WithBucketId(bucket.Id)

	e := NewEntry().WithKey(key).WithMeta(meta).WithValue(value)
	tx.submitEntry(ds, bucket.Name, e)
	tx.size += e.Size()
}

// setStatusCommitting will change the tx status to txStatusCommitting
// setStatusCommitting 将Tx修改为提交状态
func (tx *Tx) setStatusCommitting() {
	status := txStatusCommitting
	tx.status.Store(status)
}

// setStatusClosed will change the tx status to txStatusClosed
// setStatusClosed 将Tx修改为关闭状态
func (tx *Tx) setStatusClosed() {
	status := txStatusClosed
	tx.status.Store(status)
}

// setStatusRunning will change the tx status to txStatusRunning
// setStatusRunning 将Tx修改为执行状态
func (tx *Tx) setStatusRunning() {
	status := txStatusRunning
	tx.status.Store(status)
}

// isRunning will check if the tx status is txStatusRunning
// isRunning 检查Tx是否处于执行状态
func (tx *Tx) isRunning() bool {
	status := tx.status.Load().(int)
	return status == txStatusRunning
}

// isCommitting will check if the tx status is txStatusCommitting
// isCommitting 检查Tx是否处于提交状态
func (tx *Tx) isCommitting() bool {
	status := tx.status.Load().(int)
	return status == txStatusCommitting
}

// isClosed will check if the tx status is txStatusClosed
// isClosed 检查Tx是否处于关闭状态
func (tx *Tx) isClosed() bool {
	status := tx.status.Load().(int)
	return status == txStatusClosed
}

// buildIdxes 构造索引
func (tx *Tx) buildIdxes(records []*Record, entries []*Entry) error {
	// 循环获取所有元素,并根据元素中存储的元数据中存储的数据类型来构造索引
	for i, entry := range entries {
		meta := entry.Meta
		var err error
		switch meta.Ds {
		case DataStructureBTree:
			err = tx.db.buildBTreeIdx(records[i], entry)
		case DataStructureList:
			err = tx.db.buildListIdx(records[i], entry)
		case DataStructureSet:
			err = tx.db.buildSetIdx(records[i], entry)
		case DataStructureSortedSet:
			err = tx.db.buildSortedSetIdx(records[i], entry)
		}

		if err != nil {
			return err
		}

		tx.db.KeyCount++
	}
	return nil
}

// putBucket 将桶写入桶的内存索引
func (tx *Tx) putBucket(b *Bucket) error {
	// 如果该数据类型ds的map不存在,则新建一个map
	if _, exist := tx.pendingBucketList[b.Ds]; !exist {
		tx.pendingBucketList[b.Ds] = map[BucketName]*Bucket{}
	}
	// 从桶的待写数组中取出对应类型的map
	bucketInDs := tx.pendingBucketList[b.Ds]
	// 将桶放入map
	bucketInDs[b.Name] = b
	return nil
}

// SubmitBucket 提交桶
func (tx *Tx) SubmitBucket() error {
	// 分配bucketSubmitRequest类型变量的内存空间
	bucketReqs := make([]*bucketSubmitRequest, 0)
	// 循环所有待提交桶,构造bucketSubmitRequest,并添加进bucketReqs数组
	for ds, mapper := range tx.pendingBucketList {
		for name, bucket := range mapper {
			req := &bucketSubmitRequest{
				ds:     ds,
				name:   name,
				bucket: bucket,
			}
			bucketReqs = append(bucketReqs, req)
		}
	}
	// 提交桶
	return tx.db.bm.SubmitPendingBucketChange(bucketReqs)
}

// buildBucketInIndex build indexes on creation and deletion of buckets
// buildBucketInIndex 在创建和删除存储桶时建立索引
func (tx *Tx) buildBucketInIndex() error {
	for _, mapper := range tx.pendingBucketList {
		for _, bucket := range mapper {
			if bucket.Meta.Op == BucketInsertOperation {
				switch bucket.Ds {
				case DataStructureBTree:
					tx.db.Index.bTree.getWithDefault(bucket.Id)
				case DataStructureList:
					tx.db.Index.list.getWithDefault(bucket.Id)
				case DataStructureSet:
					tx.db.Index.set.getWithDefault(bucket.Id)
				case DataStructureSortedSet:
					tx.db.Index.sortedSet.getWithDefault(bucket.Id, tx.db)
				default:
					return ErrDataStructureNotSupported
				}
			} else if bucket.Meta.Op == BucketDeleteOperation {
				switch bucket.Ds {
				case DataStructureBTree:
					tx.db.Index.bTree.delete(bucket.Id)
				case DataStructureList:
					tx.db.Index.list.delete(bucket.Id)
				case DataStructureSet:
					tx.db.Index.set.delete(bucket.Id)
				case DataStructureSortedSet:
					tx.db.Index.sortedSet.delete(bucket.Id)
				default:
					return ErrDataStructureNotSupported
				}
			}
		}
	}
	return nil
}

// getChangeCountInEntriesChanges 获取被修改元素的数量
func (tx *Tx) getChangeCountInEntriesChanges() (int64, error) {
	var res int64
	for _, entriesInDS := range tx.pendingWrites.entriesInBTree {
		for _, entry := range entriesInDS {
			curRecordCnt, err := tx.getEntryNewAddRecordCount(entry)
			if err != nil {
				return res, nil
			}
			res += curRecordCnt
		}
	}
	for _, entriesInDS := range tx.pendingWrites.entries {
		for _, entries := range entriesInDS {
			for _, entry := range entries {
				curRecordCnt, err := tx.getEntryNewAddRecordCount(entry)
				if err != nil {
					return res, err
				}
				res += curRecordCnt
			}
		}
	}
	return res, nil
}

// getChangeCountInBucketChanges 获取所有被修改桶的数量
func (tx *Tx) getChangeCountInBucketChanges() int64 {
	var res int64
	f := func(bucket *Bucket) error {
		bucketId := bucket.Id
		if bucket.Meta.Op == BucketDeleteOperation {
			switch bucket.Ds {
			case DataStructureBTree:
				if bTree, ok := tx.db.Index.bTree.idx[bucketId]; ok {
					res -= int64(bTree.Count())
				}
			case DataStructureSet:
				if set, ok := tx.db.Index.set.idx[bucketId]; ok {
					for key := range set.M {
						res -= int64(set.SCard(key))
					}
				}
			case DataStructureSortedSet:
				if sortedSet, ok := tx.db.Index.sortedSet.idx[bucketId]; ok {
					for key := range sortedSet.M {
						curLen, _ := sortedSet.ZCard(key)
						res -= int64(curLen)
					}
				}
			case DataStructureList:
				if list, ok := tx.db.Index.list.idx[bucketId]; ok {
					for key := range list.Items {
						curLen, _ := list.Size(key)
						res -= int64(curLen)
					}
				}
			default:
				panic(fmt.Sprintf("there is an unexpected data structure that is unimplemented in our database.:%d", bucket.Ds))
			}
		}
		return nil
	}
	_ = tx.pendingBucketList.rangeBucket(f)
	return res
}

// 获取桶的状态
func (tx *Tx) getBucketStatus(ds Ds, name BucketName) BucketStatus {
	if len(tx.pendingBucketList) > 0 {
		if bucketInDs, exist := tx.pendingBucketList[ds]; exist {
			if bucket, exist := bucketInDs[name]; exist {
				switch bucket.Meta.Op {
				case BucketInsertOperation:
					return BucketStatusNew
				case BucketDeleteOperation:
					return BucketStatusDeleted
				case BucketUpdateOperation:
					return BucketStatusUpdated
				}
			}
		}
	}
	if tx.db.bm.ExistBucket(ds, name) {
		return BucketStatusExistAlready
	}
	return BucketStatusUnknown
}

// findEntryAndItsStatus finds the latest status for the certain Entry in Tx
// findEntryAndItsStatus  找到在事务中被创建元素的最后一个状态
func (tx *Tx) findEntryAndItsStatus(ds Ds, bucket BucketName, key string) (EntryStatus, *Entry) {
	if tx.pendingWrites.size == 0 {
		return NotFoundEntry, nil
	}
	pendingWriteEntries := tx.pendingWrites.entriesInBTree
	if pendingWriteEntries == nil {
		return NotFoundEntry, nil
	}
	if pendingWriteEntries[bucket] == nil {
		return NotFoundEntry, nil
	}
	entries := pendingWriteEntries[bucket]
	if entry, exist := entries[key]; exist {
		switch entry.Meta.Flag {
		case DataDeleteFlag:
			return EntryDeleted, nil
		default:
			return EntryUpdated, entry
		}
	}
	return NotFoundEntry, nil
}
