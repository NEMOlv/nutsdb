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
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"
	"strings"

	"github.com/xujiajun/utils/strconv2"
)

var (
	ErrPayLoadSizeMismatch   = errors.New("the payload size in Meta mismatch with the payload size needed")
	ErrHeaderSizeOutOfBounds = errors.New("the header size is out of bounds")
)

const (
	MaxEntryHeaderSize = 4 + binary.MaxVarintLen32*3 + binary.MaxVarintLen64*3 + binary.MaxVarintLen16*3
	MinEntryHeaderSize = 4 + 9
)

type (
	// Entry represents the data item.
	Entry struct {
		Key   []byte
		Value []byte
		Meta  *MetaData
	}
)

// Size returns the size of the entry.
func (e *Entry) Size() int64 {
	return e.Meta.Size() + int64(e.Meta.KeySize+e.Meta.ValueSize)
}

// Encode returns the slice after the entry be encoded.
//
//	the entry stored format:
//	|----------------------------------------------------------------------------------------------------------|
//	|  crc  | timestamp | ksz | valueSize | flag  | TTL  | status | ds   | txId |  bucketId |  key  | value    |
//	|----------------------------------------------------------------------------------------------------------|
//	| uint32| uint64  |uint32 |  uint32 | uint16  | uint32| uint16 | uint16 |uint64 | uint64 | []byte | []byte |
//	|----------------------------------------------------------------------------------------------------------|
//
// Encode 返回被编码的数组
func (e *Entry) Encode() []byte {
	// key和value的大小
	keySize := e.Meta.KeySize
	valueSize := e.Meta.ValueSize

	// 创建一个最大头部大小+kv键值对大小的字节数组
	buf := make([]byte, MaxEntryHeaderSize+keySize+valueSize)

	// 放入头部元数据
	index := e.setEntryHeaderBuf(buf)
	// 放入key
	copy(buf[index:], e.Key)
	index += int(keySize)
	// 放入value
	copy(buf[index:], e.Value)
	index += int(valueSize)
	// 去除空余部分
	buf = buf[:index]
	// 计算并放入检验和
	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// setEntryHeaderBuf sets the entry header buff.
// setEntryHeaderBuf 将元素的头部元数据写入buffer
func (e *Entry) setEntryHeaderBuf(buf []byte) int {
	index := 4

	index += binary.PutUvarint(buf[index:], e.Meta.Timestamp)
	index += binary.PutUvarint(buf[index:], uint64(e.Meta.KeySize))
	index += binary.PutUvarint(buf[index:], uint64(e.Meta.ValueSize))
	index += binary.PutUvarint(buf[index:], uint64(e.Meta.Flag))
	index += binary.PutUvarint(buf[index:], uint64(e.Meta.TTL))
	index += binary.PutUvarint(buf[index:], uint64(e.Meta.Status))
	index += binary.PutUvarint(buf[index:], uint64(e.Meta.Ds))
	index += binary.PutUvarint(buf[index:], e.Meta.TxID)
	index += binary.PutUvarint(buf[index:], e.Meta.BucketId)

	return index
}

// IsZero checks if the entry is zero or not.
// IsZero 检查元素是否为空
func (e *Entry) IsZero() bool {
	if e.Meta.Crc == 0 && e.Meta.KeySize == 0 && e.Meta.ValueSize == 0 && e.Meta.Timestamp == 0 {
		return true
	}
	return false
}

// GetCrc returns the crc at given buf slice.
// GetCrc  todo 返回字节数组的校验和
func (e *Entry) GetCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)

	return crc
}

// ParsePayload means this function will parse a byte array to bucket, key, size of an entry
// ParsePayload 该函数将会从字节数组中解析出包括bucket key entry并传入entry
func (e *Entry) ParsePayload(data []byte) error {
	meta := e.Meta
	keyLowBound := 0
	keyHighBound := meta.KeySize
	valueLowBound := keyHighBound
	valueHighBound := meta.KeySize + meta.ValueSize

	// parse key
	// 解析key
	e.Key = data[keyLowBound:keyHighBound]
	// parse value
	// 解析value
	e.Value = data[valueLowBound:valueHighBound]
	return nil
}

// checkPayloadSize checks the payload size
// checkPayloadSize 检查有效载荷大小
func (e *Entry) checkPayloadSize(size int64) error {
	// 如果元数据中存储的有效载荷大小不等于传入的有效载荷大小,则返回ErrPayLoadSizeMismatch
	if e.Meta.PayloadSize() != size {
		return ErrPayLoadSizeMismatch
	}
	return nil
}

// ParseMeta parse Meta object to entry
// ParseMeta 从entry中解析出元数据
func (e *Entry) ParseMeta(buf []byte) (int64, error) {
	// If the length of the header is less than MinEntryHeaderSize,
	// it means that the final remaining capacity of the file is not enough to write a record,
	// and an error needs to be returned.
	if len(buf) < MinEntryHeaderSize {
		return 0, ErrHeaderSizeOutOfBounds
	}

	e.Meta = NewMetaData()

	e.Meta.WithCrc(binary.LittleEndian.Uint32(buf[0:4]))

	index := 4

	timestamp, n := binary.Uvarint(buf[index:])
	index += n
	keySize, n := binary.Uvarint(buf[index:])
	index += n
	valueSize, n := binary.Uvarint(buf[index:])
	index += n
	flag, n := binary.Uvarint(buf[index:])
	index += n
	ttl, n := binary.Uvarint(buf[index:])
	index += n
	status, n := binary.Uvarint(buf[index:])
	index += n
	ds, n := binary.Uvarint(buf[index:])
	index += n
	txId, n := binary.Uvarint(buf[index:])
	index += n
	bucketId, n := binary.Uvarint(buf[index:])
	index += n

	e.Meta.
		WithTimeStamp(timestamp).
		WithKeySize(uint32(keySize)).
		WithValueSize(uint32(valueSize)).
		WithFlag(uint16(flag)).
		WithTTL(uint32(ttl)).
		WithStatus(uint16(status)).
		WithDs(uint16(ds)).
		WithTxID(txId).
		WithBucketId(bucketId)

	return int64(index), nil
}

// isFilter to confirm if this entry is can be filtered
func (e *Entry) isFilter() bool {
	meta := e.Meta
	var filterDataSet = []uint16{
		DataDeleteFlag,
		DataRPopFlag,
		DataLPopFlag,
		DataLRemFlag,
		DataLTrimFlag,
		DataZRemFlag,
		DataZRemRangeByRankFlag,
		DataZPopMaxFlag,
		DataZPopMinFlag,
		DataLRemByIndex,
	}
	return OneOfUint16Array(meta.Flag, filterDataSet)
}

// valid check the entry fields valid or not
// valid 检查键值对是否有效
func (e *Entry) valid() error {
	// 如果key为空,返回key为空的错误
	if len(e.Key) == 0 {
		return ErrKeyEmpty
	}
	// 如果key或value超过最大值发明会数据大小超越的错误
	if len(e.Key) > MAX_SIZE || len(e.Value) > MAX_SIZE {
		return ErrDataSizeExceed
	}
	return nil
}

// NewEntry new Entry Object
func NewEntry() *Entry {
	return new(Entry)
}

// WithKey set key to Entry
func (e *Entry) WithKey(key []byte) *Entry {
	e.Key = key
	return e
}

// WithValue set value to Entry
func (e *Entry) WithValue(value []byte) *Entry {
	e.Value = value
	return e
}

// WithMeta set Meta to Entry
func (e *Entry) WithMeta(meta *MetaData) *Entry {
	e.Meta = meta
	return e
}

// GetTxIDBytes return the bytes of TxID
func (e *Entry) GetTxIDBytes() []byte {
	return []byte(strconv2.Int64ToStr(int64(e.Meta.TxID)))
}

func (e *Entry) IsBelongsToBPlusTree() bool {
	return e.Meta.IsBPlusTree()
}

func (e *Entry) IsBelongsToList() bool {
	return e.Meta.IsList()
}

func (e *Entry) IsBelongsToSet() bool {
	return e.Meta.IsSet()
}

func (e *Entry) IsBelongsToSortSet() bool {
	return e.Meta.IsSortSet()
}

// Entries represents entries
type Entries []*Entry

func (e Entries) Len() int { return len(e) }

func (e Entries) Less(i, j int) bool {
	l := string(e[i].Key)
	r := string(e[j].Key)

	return strings.Compare(l, r) == -1
}

func (e Entries) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

func (e Entries) processEntriesScanOnDisk() (result []*Entry) {
	sort.Sort(e)
	for _, ele := range e {
		curE := ele
		if !IsExpired(curE.Meta.TTL, curE.Meta.Timestamp) && curE.Meta.Flag != DataDeleteFlag {
			result = append(result, curE)
		}
	}

	return result
}

func (e Entries) ToCEntries(lFunc func(l, r string) bool) CEntries {
	return CEntries{
		Entries:  e,
		LessFunc: lFunc,
	}
}

type CEntries struct {
	Entries
	LessFunc func(l, r string) bool
}

func (c CEntries) Len() int { return len(c.Entries) }

func (c CEntries) Less(i, j int) bool {
	l := string(c.Entries[i].Key)
	r := string(c.Entries[j].Key)
	if c.LessFunc != nil {
		return c.LessFunc(l, r)
	}

	return c.Entries.Less(i, j)
}

func (c CEntries) Swap(i, j int) { c.Entries[i], c.Entries[j] = c.Entries[j], c.Entries[i] }

func (c CEntries) processEntriesScanOnDisk() (result []*Entry) {
	sort.Sort(c)
	for _, ele := range c.Entries {
		curE := ele
		if !IsExpired(curE.Meta.TTL, curE.Meta.Timestamp) && curE.Meta.Flag != DataDeleteFlag {
			result = append(result, curE)
		}
	}

	return result
}

type EntryWhenRecovery struct {
	Entry
	fid int64
	off int64
}

// dataInTx 事务中的数据？
type dataInTx struct {
	es       []*EntryWhenRecovery
	txId     uint64
	startOff int64
}

func (dt *dataInTx) isSameTx(e *EntryWhenRecovery) bool {
	return dt.txId == e.Meta.TxID
}

func (dt *dataInTx) appendEntry(e *EntryWhenRecovery) {
	dt.es = append(dt.es, e)
}

func (dt *dataInTx) reset() {
	dt.es = make([]*EntryWhenRecovery, 0)
	dt.txId = 0
}
