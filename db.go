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
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/xujiajun/utils/filesystem"
	"github.com/xujiajun/utils/strconv2"
)

// ScanNoLimit represents the data scan no limit flag
// ScanNoLimit 代表没有限制的扫描数据
const ScanNoLimit int = -1
const KvWriteChCapacity = 1000
const FLockName = "nutsdb-flock"

type (
	// DB represents a collection of buckets that persist on disk.
	// DB 表示在磁盘上持久存在的bucket集合。
	DB struct {
		// the database options
		// 数据库选项
		opt Options
		// 索引
		Index *index
		// 活跃文件
		ActiveFile *DataFile
		// 最大文件ID
		MaxFileID int64
		// 读写锁
		mu sync.RWMutex
		// 所有key的数量，包括过期、删除、重复的key
		KeyCount int // total key number ,include expired, deleted, repeated.
		// 关闭标识
		closed bool
		// 合并标识
		isMerging bool
		// 文件管理器
		fm *fileManager
		// 文件锁
		flock *flock.Flock
		// 提交buffer
		commitBuffer *bytes.Buffer
		// 合并开始通道
		mergeStartCh chan struct{}
		// 合并结束通道
		mergeEndCh chan error
		// 合并工作通道
		mergeWorkCloseCh chan struct{}
		// 写通道
		writeCh chan *request
		// 生命周期管理器
		tm *ttlManager
		// current valid record count, exclude deleted, repeated
		// 有效记录，排除了删除、重复数据
		RecordCount int64
		// bucket管理器
		bm *BucketManager
		// 使用LRU缓存的 HintKeyAndRAMIdxMode
		hintKeyAndRAMIdxModeLru *LRUCache // lru cache for HintKeyAndRAMIdxMode
	}
)

// open returns a newly initialized DB object.
// open 返回一个新初始化的DB实例
func open(opt Options) (*DB, error) {
	db := &DB{
		MaxFileID:               0,
		opt:                     opt,
		KeyCount:                0,
		closed:                  false,
		Index:                   newIndex(),
		fm:                      newFileManager(opt.RWMode, opt.MaxFdNumsInCache, opt.CleanFdsCacheThreshold, opt.SegmentSize),
		mergeStartCh:            make(chan struct{}),
		mergeEndCh:              make(chan error),
		mergeWorkCloseCh:        make(chan struct{}),
		writeCh:                 make(chan *request, KvWriteChCapacity),
		tm:                      newTTLManager(opt.ExpiredDeleteType),
		hintKeyAndRAMIdxModeLru: NewLruCache(opt.HintKeyAndRAMIdxCacheSize),
	}

	db.commitBuffer = createNewBufferWithSize(int(db.opt.CommitBufferSize))

	// 创建数据库目录
	if ok := filesystem.PathIsExist(db.opt.Dir); !ok {
		if err := os.MkdirAll(db.opt.Dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 新建文件锁
	fileLock := flock.New(filepath.Join(opt.Dir, FLockName))
	if ok, err := fileLock.TryLock(); err != nil {
		return nil, err
	} else if !ok {
		return nil, ErrDirLocked
	}

	db.flock = fileLock

	// 新建bucket管理器
	if bm, err := NewBucketManager(opt.Dir); err == nil {
		db.bm = bm
	} else {
		return nil, err
	}

	// 重建bucket管理器
	if err := db.rebuildBucketManager(); err != nil {
		return nil, fmt.Errorf("db.rebuildBucketManager err:%s", err)
	}

	// 重建索引
	if err := db.buildIndexes(); err != nil {
		return nil, fmt.Errorf("db.buildIndexes error: %s", err)
	}

	// 启动新进程
	// 合并worker进程
	go db.mergeWorker()
	// 写入进程
	go db.doWrites()
	// 事务管理进程
	go db.tm.run()

	return db, nil
}

// Open returns a newly initialized DB object with Option.
// Open 返回一个包含Option的新初始化的DB实例
//
//	该函数对外暴露
func Open(options Options, ops ...Option) (*DB, error) {
	opts := &options
	// 对options进行某些操作，传入的ops是操作opts的函数
	for _, do := range ops {
		do(opts)
	}
	return open(*opts)
}

// Update executes a function within a managed read/write transaction.
// Update 在受管理的读写事务中执行fn函数。
func (db *DB) Update(fn func(tx *Tx) error) error {
	// 如果入参fn为空则返回ErrFn
	if fn == nil {
		return ErrFn
	}

	return db.managed(true, fn)
}

// View executes a function within a managed read-only transaction.
// View 在受管理的读事务中执行fn函数。
func (db *DB) View(fn func(tx *Tx) error) error {
	// 如果入参fn为空则返回ErrFn
	if fn == nil {
		return ErrFn
	}

	return db.managed(false, fn)
}

// Backup copies the database to file directory at the given dir.
// Backup 将数据库复制到指定目录下的文件目录
func (db *DB) Backup(dir string) error {
	return db.View(func(tx *Tx) error {
		return filesystem.CopyDir(db.opt.Dir, dir)
	})
}

// BackupTarGZ Backup copy the database to writer.
// BackupTarGZ 将数据库备份复制到写入器。
func (db *DB) BackupTarGZ(w io.Writer) error {
	return db.View(func(tx *Tx) error {
		return tarGZCompress(w, db.opt.Dir)
	})
}

// Close releases all db resources.
// Close 释放所有DB资源
func (db *DB) Close() error {
	// 写锁
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断数据库是否关闭
	if db.closed {
		return ErrDBClosed
	}

	// 数据库关闭标识
	db.closed = true

	// 释放数据库资源
	err := db.release()
	if err != nil {
		return err
	}

	return nil
}

// release set all obj in the db instance to nil
// release 将数据库实例中的所有对象置空
func (db *DB) release() error {
	// 是否启动GC
	GCEnable := db.opt.GCWhenClose

	// 释放活跃文件的读写管理器
	err := db.ActiveFile.rwManager.Release()
	if err != nil {
		return err
	}

	// 索引置空
	db.Index = nil

	// 活跃文件置空
	db.ActiveFile = nil

	// 关闭文件管理器
	err = db.fm.close()
	if err != nil {
		return err
	}

	// 向mergeWorkCloseCh通道发送一个空结构体，应该是代表关闭
	db.mergeWorkCloseCh <- struct{}{}

	// 如果文件锁是锁定状态，则返回ErrDirUnlocked
	if !db.flock.Locked() {
		return ErrDirUnlocked
	}
	// 释放文件锁
	err = db.flock.Unlock()
	if err != nil {
		return err
	}

	// 文件管理器置空
	db.fm = nil

	// 关闭事务管理器
	db.tm.close()

	// 如果开启GC则执行GC操作
	if GCEnable {
		runtime.GC()
	}

	return nil
}

// getValueByRecord 从记录中获取值
func (db *DB) getValueByRecord(record *Record) ([]byte, error) {
	// 如果记录为空，则返回 nil, ErrRecordIsNil
	if record == nil {
		return nil, ErrRecordIsNil
	}

	// 如果记录的值不为空，则返回 记录的值,nil
	if record.Value != nil {
		return record.Value, nil
	}

	// firstly we find data in cache
	// 首先我们从缓存中查找数据
	if db.getHintKeyAndRAMIdxCacheSize() > 0 {
		if value := db.hintKeyAndRAMIdxModeLru.Get(record); value != nil {
			return value.(*Entry).Value, nil
		}
	}

	// 通过文件ID和数据库目录获得文件的全路径
	dirPath := getDataPath(record.FileID, db.opt.Dir)
	// 根据dirPath获取数据文件
	df, err := db.fm.getDataFile(dirPath, db.opt.SegmentSize)
	if err != nil {
		return nil, err
	}

	// defer释放读写管理器rwManager
	defer func(rwManager RWManager) {
		err := rwManager.Release()
		if err != nil {
			return
		}
	}(df.rwManager)

	// payloadSize = KeySize+ValueSize
	payloadSize := int64(len(record.Key)) + int64(record.ValueSize)
	// 调用df.ReadEntry方法，传入记录写入偏移和有效数据大小，获取记录的值
	item, err := df.ReadEntry(int(record.DataPos), payloadSize)
	if err != nil {
		return nil, fmt.Errorf("read err. pos %d, key %s, err %s", record.DataPos, record.Key, err)
	}

	// saved in cache
	// 将值放入缓存
	if db.getHintKeyAndRAMIdxCacheSize() > 0 {
		db.hintKeyAndRAMIdxModeLru.Add(record, item)
	}

	// 返回记录的值
	return item.Value, nil
}

// commitTransaction 提交事务
func (db *DB) commitTransaction(tx *Tx) (err error) {
	// defer后处理
	defer func() {
		var panicked bool
		// 如果发生崩溃，通过执行recover()捕获异常并继续执行
		if r := recover(); r != nil {
			// resume normal execution
			// 恢复正常执行
			panicked = true
		}
		// 如果 panicked 或 err 不为空，则执行回滚
		if panicked || err != nil {
			// log.Fatal("panicked=", panicked, ", err=", err)
			// 日志记录崩溃和错误
			if errRollback := tx.Rollback(); errRollback != nil {
				err = errRollback
			}
		}
	}()

	// commit current tx
	// 提交当前事务
	tx.lock()
	tx.setStatusRunning()
	if err = tx.Commit(); err != nil {
		// log.Fatal("txCommit fail,err=", err)
		return
	}

	return
}

// writeRequests 写请求
func (db *DB) writeRequests(reqs []*request) (err error) {
	// 如果请求为空, 则返回nil
	if len(reqs) == 0 {
		return
	}

	// 声明一个内部函数done, 入参为错误变量err, 在函数体内循环请求reqs，将err赋予每个请求
	done := func(err error) {
		for _, r := range reqs {
			r.Err = err
			r.Wg.Done()
		}
	}

	// 循环每一个请求，并对每一个请求调用事务，然后用db提交事务，如果cerr不为空，则将其赋予err
	for _, req := range reqs {
		tx := req.tx
		cerr := db.commitTransaction(tx)
		if cerr != nil {
			err = cerr
		}
	}

	// 处理err
	done(err)

	return
}

// MaxBatchCount returns max possible entries in batch
// getMaxBatchCount 返回batch中可能的最大条目数
func (db *DB) getMaxBatchCount() int64 {
	return db.opt.MaxBatchCount
}

// MaxBatchSize returns max possible batch size
// getMaxBatchSize 返回batch可能的最大大小
func (db *DB) getMaxBatchSize() int64 {
	return db.opt.MaxBatchSize
}

// getMaxWriteRecordCount 返回最大写入数量
func (db *DB) getMaxWriteRecordCount() int64 {
	return db.opt.MaxWriteRecordCount
}

// getHintKeyAndRAMIdxCacheSize 返回HintKey和RAMIdx的缓存大小
func (db *DB) getHintKeyAndRAMIdxCacheSize() int {
	return db.opt.HintKeyAndRAMIdxCacheSize
}

// doWrites 执行写入
func (db *DB) doWrites() {
	// 感觉这里通道的作用像锁的作用
	// 等待通道，通道大小为1
	pendingCh := make(chan struct{}, 1)
	// 声明内部函数writeRequests, 入参为请求reqs, 尝试执行writeRequests，如果出现错误则日志记录
	// 如果内部发生多次错误，只会记录并返回最后一次错误
	writeRequests := func(reqs []*request) {
		if err := db.writeRequests(reqs); err != nil {
			log.Fatal("writeRequests fail, err=", err)
		}
		// 无论是否成功，均从等待通道中读出数据，相当于释放锁
		<-pendingCh
	}

	// 声明reqs数组，初始化长度为0，容量为10
	reqs := make([]*request, 0, 10)
	// 声明当前请求指针
	var r *request
	// 声明ok变量
	var ok bool

	for {
		// 从db.writeCh通道读出数据，如果读不出数据，则跳转到closedCase代码块
		r, ok = <-db.writeCh
		if !ok {
			goto closedCase
		}

		// 整体逻辑流程是以下三个步骤的循环：
		//		pendingCh获取锁，并对reqs数组进行写入。
		//		执行写入操作时，不断从写入通道writeCh获取请求指针，并将请求指针添加进reqs数组。
		//		当上一轮写入操作结束后，pendingCh释放锁。
		for {
			// 将当前请求指针添加进reqs数组
			reqs = append(reqs, r)

			// 如果reqs数组长度大于3倍的KvWriteCh通道容量，则
			if len(reqs) >= 3*KvWriteChCapacity {
				// blocking
				// 进行阻塞，相当于获取锁，然后跳转writeCase
				pendingCh <- struct{}{}
				goto writeCase
			}

			select {
			// Either push to pending, or continue to pick from writeCh.
			// 要么阻塞pendingCh通道并跳转writeCase，要么从writeCh通道中读取数据，如果通道被关闭，则跳转closedCase
			case r, ok = <-db.writeCh:
				if !ok {
					goto closedCase
				}
			case pendingCh <- struct{}{}:
				goto writeCase
			}
		}

	closedCase:
		// All the pending request are drained.
		// Don't close the writeCh, because it has be used in several places.
		// 所有待处理请求都已耗尽。
		// 不要关闭 writeCh，因为它可以在多个地方使用。
		for {
			// 确保所有写请求都已经添加到reqs数组，当写通道writeCh无法再读出数据时，执行最后一次写入，并结束doWrites的执行
			select {
			case r = <-db.writeCh:
				reqs = append(reqs, r)
			default:
				pendingCh <- struct{}{} // Push to pending before doing write.
				writeRequests(reqs)
				return
			}
		}

	writeCase:
		// 启动其他协程执行writeRequests，执行完毕后重置reqs数组
		go writeRequests(reqs)
		reqs = make([]*request, 0, 10)
	}
}

// setActiveFile sets the ActiveFile (DataFile object).
// setActiveFile 设置活跃文件
func (db *DB) setActiveFile() (err error) {
	// 通过最大文件ID和数据库目录获得活跃文件的全路径
	activeFilePath := getDataPath(db.MaxFileID, db.opt.Dir)
	// 获取活跃文件
	db.ActiveFile, err = db.fm.getDataFile(activeFilePath, db.opt.SegmentSize)
	if err != nil {
		return
	}

	db.ActiveFile.fileID = db.MaxFileID

	return
}

// getMaxFileIDAndFileIds returns max fileId and fileIds.
// getMaxFileIDAndFileIds 返回最大文件ID和文件ID数组
func (db *DB) getMaxFileIDAndFileIDs() (maxFileID int64, dataFileIds []int) {
	// 从数据库目录中获取所有文件
	files, _ := os.ReadDir(db.opt.Dir)

	// 如果文件数组为空，则返回0, nil
	if len(files) == 0 {
		return 0, nil
	}

	// 循环获取单个文件
	for _, file := range files {
		// 获取文件名
		filename := file.Name()
		// 获取文件后缀
		fileSuffix := path.Ext(path.Base(filename))
		// 如果不是数据文件后缀则跳过
		if fileSuffix != DataSuffix {
			continue
		}
		// 去除文件名后缀
		filename = strings.TrimSuffix(filename, DataSuffix)
		// 将文件名从string转为int
		id, _ := strconv2.StrToInt(filename)
		// 并添加进文件ID数组
		dataFileIds = append(dataFileIds, id)
	}

	// 如果文件名数组为空，则返回0, nil
	if len(dataFileIds) == 0 {
		return 0, nil
	}

	// 对文件数组排序
	sort.Ints(dataFileIds)
	// 获得最大文件ID
	maxFileID = int64(dataFileIds[len(dataFileIds)-1])

	return
}

// parseDataFiles 解析数据文件
func (db *DB) parseDataFiles(dataFileIds []int) (err error) {
	var (
		// 写入偏移
		off int64
		// todo 文件恢复结构体？
		f *fileRecovery
		// 文件ID
		fID int64
		// todo 事务中的数据？
		dataInTx dataInTx
	)

	// 解析事务中的数据
	parseDataInTx := func() error {
		for _, entry := range dataInTx.es {
			// if this bucket is not existed in bucket manager right now
			// its because it already deleted in the feature WAL log.
			// so we can just ignore here.
			// 如果这个bucket当前不存在bucket管理器，是因为它已经在未来的预写日志中被删除了
			bucketId := entry.Meta.BucketId
			if _, err := db.bm.GetBucketById(bucketId); errors.Is(err, ErrBucketNotExist) {
				continue
			}
			// 使用文件ID和写入偏移，从Mode中创建Record
			record := db.createRecordByModeWithFidAndOff(entry.fid, uint64(entry.off), &entry.Entry)
			// 重建索引
			if err = db.buildIdxes(record, &entry.Entry); err != nil {
				return err
			}

			db.KeyCount++

		}
		return nil
	}

	// 从文件中读取元素(实际上是在重建内存索引)
	readEntriesFromFile := func() error {
		for {
			// 使用 readEntry方法，传入写入偏移，读取元素
			entry, err := f.readEntry(off)
			if err != nil {
				// whatever which logic branch it will choose, we will release the fd.
				// 无论选择哪个分支，都将释放fd
				_ = f.release()
				if errors.Is(err, io.EOF) || errors.Is(err, ErrIndexOutOfBound) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrEntryZero) || errors.Is(err, ErrHeaderSizeOutOfBounds) {
					break
				}
				if off >= db.opt.SegmentSize {
					break
				}

				return err
			}

			if entry == nil {
				break
			}

			// 恢复时的元素
			entryWhenRecovery := &EntryWhenRecovery{
				Entry: *entry,
				fid:   fID,
				off:   off,
			}
			// 如果dataInTx.txId == 0，则将entryWhenRecovery加入es(entries)数组，并设定起始写入偏移(startOff)
			if dataInTx.txId == 0 {
				dataInTx.appendEntry(entryWhenRecovery)
				dataInTx.txId = entry.Meta.TxID
				dataInTx.startOff = off
				//	否则判断entryWhenRecovery是否和dataInTx具有相同的TxID，有则加入es(entries)数组
			} else if dataInTx.isSameTx(entryWhenRecovery) {
				dataInTx.appendEntry(entryWhenRecovery)
			}

			// 如果entry的状态是已提交
			if entry.Meta.Status == Committed {
				err := parseDataInTx()
				if err != nil {
					return err
				}
				dataInTx.reset()
				dataInTx.startOff = off
			}

			// 如果dataInTx和entryWhenRecovery具有不同TxID
			// 立刻重置dataInTx并将dataInTx.startOff设置为当前写入偏移off
			if !dataInTx.isSameTx(entryWhenRecovery) {
				dataInTx.reset()
				dataInTx.startOff = off
			}

			off += entry.Size()
		}

		// 如果文件ID和最大文件ID相等，则活跃文件的实际大小和写入偏移，均用当前写入偏移表示
		if fID == db.MaxFileID {
			db.ActiveFile.ActualSize = off
			db.ActiveFile.writeOff = off
		}

		return nil
	}

	// 循环获取数据文件ID
	for _, dataID := range dataFileIds {
		off = 0
		fID = int64(dataID)
		// 拼接全路径
		dataPath := getDataPath(fID, db.opt.Dir)
		// 新建FileRecovery结构体，传入全路径和结构体的BufferSize
		f, err = newFileRecovery(dataPath, db.opt.BufferSizeOfRecovery)
		if err != nil {
			return err
		}
		// 从文件中读取元素(实际上是在重建内存索引)
		err := readEntriesFromFile()
		if err != nil {
			return err
		}
	}

	// compute the valid record count and save it in db.RecordCount
	// 计算有效记录数并将其保存至db.RecordCount
	db.RecordCount, err = db.getRecordCount()
	return
}

// getRecordCount 计算有效记录数
func (db *DB) getRecordCount() (int64, error) {
	var res int64

	// Iterate through the BTree indices
	// 迭代BTree索引
	// db.Index.bTree.idx是一个map，实际上循环就是在循环map中的所有BTree
	for _, btree := range db.Index.bTree.idx {
		res += int64(btree.Count())
	}

	// Iterate through the List indices
	// 迭代List索引
	for _, listItem := range db.Index.list.idx {
		for key := range listItem.Items {
			curLen, err := listItem.Size(key)
			if err != nil {
				return res, err
			}
			res += int64(curLen)
		}
	}

	// Iterate through the Set indices
	// 迭代Set索引
	for _, setItem := range db.Index.set.idx {
		for key := range setItem.M {
			res += int64(setItem.SCard(key))
		}
	}

	// Iterate through the SortedSet indices
	// 迭代SortedSet索引
	for _, zsetItem := range db.Index.sortedSet.idx {
		for key := range zsetItem.M {
			curLen, err := zsetItem.ZCard(key)
			if err != nil {
				return res, err
			}
			res += int64(curLen)
		}
	}

	return res, nil
}

// buildBTreeIdx 重建BTree内存索引
func (db *DB) buildBTreeIdx(record *Record, entry *Entry) error {
	// 获取元素的key和元数据
	key, meta := entry.Key, entry.Meta

	// 获取bucket
	bucket, err := db.bm.GetBucketById(meta.BucketId)
	if err != nil {
		return err
	}
	bucketId := bucket.Id

	// 获取BTree
	bTree := db.Index.bTree.getWithDefault(bucketId)

	// 如果记录过期 或 该数据被删除，则删除从 生命周期管理器 和 BTree 中删除数据
	if record.IsExpired() || meta.Flag == DataDeleteFlag {
		db.tm.del(bucketId, string(key))
		bTree.Delete(key)
	} else {
		// 如果数据不是持久化的，则放入生命周期管理器，否则从生命周期管理器中删除
		// 将数据插入BTree
		if meta.TTL != Persistent {
			db.tm.add(bucketId, string(key), db.expireTime(meta.Timestamp, meta.TTL), db.buildExpireCallback(bucket.Name, key))
		} else {
			db.tm.del(bucketId, string(key))
		}
		bTree.Insert(record)
	}
	return nil
}

// expireTime 过期时间 = 创建时间+生命周期-当前时间
func (db *DB) expireTime(timestamp uint64, ttl uint32) time.Duration {
	now := time.UnixMilli(time.Now().UnixMilli())
	expireTime := time.UnixMilli(int64(timestamp))
	expireTime = expireTime.Add(time.Duration(int64(ttl)) * time.Second)
	return expireTime.Sub(now)
}

// buildIdxes 重建索引
func (db *DB) buildIdxes(record *Record, entry *Entry) error {
	// 获取元数据
	meta := entry.Meta
	// 根据元素使用的数据结构，选择不同的重建内存索引方式
	switch meta.Ds {
	case DataStructureBTree:
		return db.buildBTreeIdx(record, entry)
	case DataStructureList:
		if err := db.buildListIdx(record, entry); err != nil {
			return err
		}
	case DataStructureSet:
		if err := db.buildSetIdx(record, entry); err != nil {
			return err
		}
	case DataStructureSortedSet:
		if err := db.buildSortedSetIdx(record, entry); err != nil {
			return err
		}
	default:
		panic(fmt.Sprintf("there is an unexpected data structure that is unimplemented in our database.:%d", meta.Ds))
	}
	return nil
}

// deleteBucket 删除bucket
func (db *DB) deleteBucket(ds uint16, bucket BucketId) {
	if ds == DataStructureSet {
		db.Index.set.delete(bucket)
	}
	if ds == DataStructureSortedSet {
		db.Index.sortedSet.delete(bucket)
	}
	if ds == DataStructureBTree {
		db.Index.bTree.delete(bucket)
	}
	if ds == DataStructureList {
		db.Index.list.delete(bucket)
	}
}

// buildSetIdx builds set index when opening the DB.
// buildSetIdx 在打开DB时重建set的索引
func (db *DB) buildSetIdx(record *Record, entry *Entry) error {
	key, val, meta := entry.Key, entry.Value, entry.Meta

	bucket, err := db.bm.GetBucketById(entry.Meta.BucketId)
	if err != nil {
		return err
	}
	bucketId := bucket.Id

	s := db.Index.set.getWithDefault(bucketId)

	switch meta.Flag {
	case DataSetFlag:
		if err := s.SAdd(string(key), [][]byte{val}, []*Record{record}); err != nil {
			return fmt.Errorf("when build SetIdx SAdd index err: %s", err)
		}
	case DataDeleteFlag:
		if err := s.SRem(string(key), val); err != nil {
			return fmt.Errorf("when build SetIdx SRem index err: %s", err)
		}
	}

	return nil
}

// buildSortedSetIdx builds sorted set index when opening the DB.
// buildSortedSetIdx 在打开DB时重建 sorted set 的索引
func (db *DB) buildSortedSetIdx(record *Record, entry *Entry) error {
	key, val, meta := entry.Key, entry.Value, entry.Meta

	bucket, err := db.bm.GetBucketById(entry.Meta.BucketId)
	if err != nil {
		return err
	}
	bucketId := bucket.Id

	ss := db.Index.sortedSet.getWithDefault(bucketId, db)

	switch meta.Flag {
	case DataZAddFlag:
		keyAndScore := strings.Split(string(key), SeparatorForZSetKey)
		if len(keyAndScore) == 2 {
			key := keyAndScore[0]
			score, _ := strconv2.StrToFloat64(keyAndScore[1])
			err = ss.ZAdd(key, SCORE(score), val, record)
		}
	case DataZRemFlag:
		_, err = ss.ZRem(string(key), val)
	case DataZRemRangeByRankFlag:
		start, end := splitIntIntStr(string(val), SeparatorForZSetKey)
		err = ss.ZRemRangeByRank(string(key), start, end)
	case DataZPopMaxFlag:
		_, _, err = ss.ZPopMax(string(key))
	case DataZPopMinFlag:
		_, _, err = ss.ZPopMin(string(key))
	}

	// We don't need to panic if sorted set is not found.
	if err != nil && !errors.Is(err, ErrSortedSetNotFound) {
		return fmt.Errorf("when build sortedSetIdx err: %s", err)
	}

	return nil
}

// buildListIdx builds List index when opening the DB.
// buildListIdx 在打开DB时重建List的索引
func (db *DB) buildListIdx(record *Record, entry *Entry) error {
	key, val, meta := entry.Key, entry.Value, entry.Meta

	bucket, err := db.bm.GetBucketById(entry.Meta.BucketId)
	if err != nil {
		return err
	}
	bucketId := bucket.Id

	l := db.Index.list.getWithDefault(bucketId)

	if IsExpired(meta.TTL, meta.Timestamp) {
		return nil
	}

	switch meta.Flag {
	case DataExpireListFlag:
		t, _ := strconv2.StrToInt64(string(val))
		ttl := uint32(t)
		l.TTL[string(key)] = ttl
		l.TimeStamp[string(key)] = meta.Timestamp
	case DataLPushFlag:
		err = l.LPush(string(key), record)
	case DataRPushFlag:
		err = l.RPush(string(key), record)
	case DataLRemFlag:
		err = db.buildListLRemIdx(val, l, key)
	case DataLPopFlag:
		_, err = l.LPop(string(key))
	case DataRPopFlag:
		_, err = l.RPop(string(key))
	case DataLTrimFlag:
		newKey, start := splitStringIntStr(string(key), SeparatorForListKey)
		end, _ := strconv2.StrToInt(string(val))
		err = l.LTrim(newKey, start, end)
	case DataLRemByIndex:
		indexes, _ := UnmarshalInts(val)
		err = l.LRemByIndex(string(key), indexes)
	}

	if err != nil {
		return fmt.Errorf("when build listIdx err: %s", err)
	}

	return nil
}

func (db *DB) buildListLRemIdx(value []byte, l *List, key []byte) error {
	count, newValue := splitIntStringStr(string(value), SeparatorForListKey)

	return l.LRem(string(key), count, func(r *Record) (bool, error) {
		v, err := db.getValueByRecord(r)
		if err != nil {
			return false, err
		}
		return bytes.Equal([]byte(newValue), v), nil
	})
}

// buildIndexes builds indexes when db initialize resource.
// buildIndexes 在初始化DB资源时重建索引
func (db *DB) buildIndexes() (err error) {
	var (
		maxFileID   int64
		dataFileIds []int
	)

	maxFileID, dataFileIds = db.getMaxFileIDAndFileIDs()

	// init db.ActiveFile
	db.MaxFileID = maxFileID

	// set ActiveFile
	if err = db.setActiveFile(); err != nil {
		return
	}

	if dataFileIds == nil && maxFileID == 0 {
		return
	}

	// build hint index
	return db.parseDataFiles(dataFileIds)
}

// createRecordByModeWithFidAndOff 携带文件ID和写入偏移通过Mode创建记录
func (db *DB) createRecordByModeWithFidAndOff(fid int64, off uint64, entry *Entry) *Record {
	record := NewRecord()

	record.WithKey(entry.Key).
		WithTimestamp(entry.Meta.Timestamp).
		WithTTL(entry.Meta.TTL).
		WithTxID(entry.Meta.TxID)

	// 如果元素的索引模式为HintKeyValAndRAMIdxMode，则添加value
	if db.opt.EntryIdxMode == HintKeyValAndRAMIdxMode {
		record.WithValue(entry.Value)
	}
	// 如果元素的索引模式为HintKeyAndRAMIdxMode，则添加文件ID、写入偏移、value_size
	if db.opt.EntryIdxMode == HintKeyAndRAMIdxMode {
		record.WithFileId(fid).
			WithDataPos(off).
			WithValueSize(uint32(len(entry.Value)))
	}

	return record
}

// managed calls a block of code that is fully contained in a transaction.
// managed 调用完全包含在事务中的代码块。
// 入参：
//
//	writable：是否写入，默认写入
//	fn：事务处理函数
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	// 声明事务
	var tx *Tx

	// 开启事务
	if tx, err = db.Begin(writable); err != nil {
		return err
	}

	// 后处理：如果发生panic则使用recover恢复并记录崩溃原因
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic when executing tx, err is %+v", r)
		}
	}()

	// fn处理事务，没有错误则提交，否则回滚
	if err = fn(tx); err == nil {
		// 执行提交
		err = tx.Commit()
	} else {
		//如果错误处理器部位空则捕获错误
		if db.opt.ErrorHandler != nil {
			db.opt.ErrorHandler.HandleError(err)
		}
		// 执行回滚
		if errRollback := tx.Rollback(); errRollback != nil {
			err = fmt.Errorf("%v. Rollback err: %v", err, errRollback)
		}
	}

	return err
}

// sendToWriteCh 发送至写入通道
func (db *DB) sendToWriteCh(tx *Tx) (*request, error) {
	// 从缓冲池中获取缓冲变量并重置
	req := requestPool.Get().(*request)
	req.reset()
	req.Wg.Add(1)
	req.tx = tx
	req.IncrRef() // for db write
	// Handled in doWrites.
	// 在doWrites中处理
	db.writeCh <- req
	return req, nil
}

// checkListExpired 检查List元素是否过期
func (db *DB) checkListExpired() {
	db.Index.list.rangeIdx(func(l *List) {
		for key := range l.TTL {
			l.IsExpire(key)
		}
	})
}

// IsClose return the value that represents the status of DB
// IsClose 返回一个代表DB状态的值
func (db *DB) IsClose() bool {
	return db.closed
}

// buildExpireCallback 构建过期回调函数
func (db *DB) buildExpireCallback(bucket string, key []byte) func() {
	return func() {
		// 调用 db.Update 方法：
		err := db.Update(func(tx *Tx) error {
			b, err := tx.db.bm.GetBucket(DataStructureBTree, bucket)
			if err != nil {
				return err
			}
			bucketId := b.Id
			if db.tm.exist(bucketId, string(key)) {
				return tx.Delete(bucket, key)
			}
			return nil
		})
		if err != nil {
			log.Printf("occur error when expired deletion, error: %v", err.Error())
		}
	}
}

// rebuildBucketManager 重建bucket管理器
func (db *DB) rebuildBucketManager() error {
	// 构建bucket全路径
	bucketFilePath := db.opt.Dir + "/" + BucketStoreFileName
	// 打开bucket文件
	f, err := newFileRecovery(bucketFilePath, db.opt.BufferSizeOfRecovery)
	if err != nil {
		return nil
	}
	// 创建bucketRequest数组
	bucketRequest := make([]*bucketSubmitRequest, 0)

	for {
		// 从bucket文件中读取bucket
		bucket, err := f.readBucket()
		if err != nil {
			// whatever which logic branch it will choose, we will release the fd.
			// 无论选择哪个逻辑分支，我们都要释放文件资源
			_ = f.release()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			} else {
				return err
			}
		}
		// 将bucket包装成bucketSubmitRequest，并添加进bucketRequest数组
		bucketRequest = append(bucketRequest, &bucketSubmitRequest{
			ds:     bucket.Ds,
			name:   BucketName(bucket.Name),
			bucket: bucket,
		})
	}
	// todo 不是很理解
	if len(bucketRequest) > 0 {
		err = db.bm.SubmitPendingBucketChange(bucketRequest)
		if err != nil {
			return err
		}
	}
	return nil
}
