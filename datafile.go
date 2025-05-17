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
)

var (
	// ErrCrc is returned when crc is error
	// ErrCrc 当crc为错误时返回
	ErrCrc = errors.New("crc error")

	// ErrCapacity is returned when capacity is error.
	// ErrCapacity 当容量是错误时返回 todo 感觉英文有问题?
	ErrCapacity = errors.New("capacity error")
	// ErrEntryZero 当entry为空时返回
	ErrEntryZero = errors.New("entry is zero")
)

const (
	// DataSuffix returns the data suffix
	// DataSuffix 返回数据后缀
	DataSuffix = ".dat"
)

// DataFile records about data file information.
// DataFile 记录数据文件的信息,即数据文件元数据
type DataFile struct {
	// 数据文件的全路径
	path string
	// 文件ID
	fileID int64
	// 当前写入偏移
	writeOff int64
	// 文件实际大小
	ActualSize int64
	// 读写管理器
	rwManager RWManager
}

// NewDataFile will return a new DataFile Object.
// NewDataFile 将会返回一个新的DataFile实例
func NewDataFile(path string, rwManager RWManager) *DataFile {
	// 传入文件全路径和读写管理器,并返回dataFile实例
	dataFile := &DataFile{
		path:      path,
		rwManager: rwManager,
	}
	return dataFile
}

// ReadEntry returns entry at the given off(offset).
// payloadSize = bucketSize + keySize + valueSize
// ReadEntry 根据给定的写入偏移返回entry
// 有效载荷大小 = 桶大小 + key大小 + value大小
func (df *DataFile) ReadEntry(off int, payloadSize int64) (e *Entry, err error) {
	// 数据大小 = 头部大小 + 有效载荷大小
	size := MaxEntryHeaderSize + payloadSize
	// Since MaxEntryHeaderSize + payloadSize may be larger than the actual entry size, it needs to be calculated
	// 如果 写入偏移+数据大小 大于数据文件大小,则令 数据大小 = 数据文件大小-写入偏移
	if int64(off)+size > df.rwManager.Size() {
		size = df.rwManager.Size() - int64(off)
	}
	// 根据数据大小分配字节数组内存空间
	buf := make([]byte, size)
	// 调用读写管理器的ReadAt方法,将数据读取至buf中
	if _, err := df.rwManager.ReadAt(buf, int64(off)); err != nil {
		return nil, err
	}

	// 新建一个空的Entry指针,即KV键值对
	e = new(Entry)

	// 通过ParseMeta方法,将元数据传入e中,并返回头部大小headerSize
	headerSize, err := e.ParseMeta(buf)
	if err != nil {
		return nil, err
	}

	// Remove the content after the Header
	// todo 删除页眉后的内容? 实际上这里做的是根据实际头部小+数据大小更新buf
	buf = buf[:int(headerSize+payloadSize)]

	// 如果e为空,则返回ErrEntryZero
	if e.IsZero() {
		return nil, ErrEntryZero
	}

	// 检查有效载荷大小
	if err := e.checkPayloadSize(payloadSize); err != nil {
		return nil, err
	}

	// 解析载荷数据
	err = e.ParsePayload(buf[headerSize:])
	if err != nil {
		return nil, err
	}

	// 根据头部数据获取crc
	crc := e.GetCrc(buf[:headerSize])
	// 如果当前crc和元数据中的crc不相等,说明数据发生损坏,返回ErrCrc
	if crc != e.Meta.Crc {
		return nil, ErrCrc
	}

	return
}

// WriteAt copies data to mapped region from the b slice starting at
// given off and returns number of bytes copied to the mapped region.
// WriteAt 写入方法 从给定的位置开始将数据从 b 片段复制到映射区域，并返回复制到映射区域的字节数。
func (df *DataFile) WriteAt(b []byte, off int64) (n int, err error) {
	return df.rwManager.WriteAt(b, off)
}

// Sync commits the current contents of the file to stable storage.
// Typically, this means flushing the file system's in-memory copy
// of recently written data to disk.
// Sync 同步方法 会将文件的当前内容提交到稳定存储器中。
// 通常，这意味着将文件系统内存中最近写入数据的副本刷新到磁盘上。
func (df *DataFile) Sync() (err error) {
	return df.rwManager.Sync()
}

// Close closes the RWManager.
// If RWManager is FileRWManager represents closes the File,
// rendering it unusable for I/O.
// If RWManager is a MMapRWManager represents Unmap deletes the memory mapped region,
// flushes any remaining changes.
// Close 关闭方法 关闭RWManager
// 如果RWManager是FileRWManager代表关闭文件,使其无法IO
// 如果RWManager是MMapRWManager代表 Unmap 删除内存映射区域，并刷新任何剩余更改。
func (df *DataFile) Close() (err error) {
	return df.rwManager.Close()
}

// Release 释放方法 todo 目前还没完全搞明白
func (df *DataFile) Release() (err error) {
	return df.rwManager.Release()
}
