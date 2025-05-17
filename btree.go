// Copyright 2023 The nutsdb Author. All rights reserved.
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
	"regexp"

	"github.com/tidwall/btree"
)

// ErrKeyNotFound is returned when the key is not in the b tree.
// ErrKeyNotFound 当key不在BTree中时返回
var ErrKeyNotFound = errors.New("key not found")

// Item kv键值对
type Item struct {
	key    []byte
	record *Record
}

// BTree BTree的封装
type BTree struct {
	btree *btree.BTreeG[*Item]
}

// NewBTree 新建BTree
func NewBTree() *BTree {
	// 返回BTree
	return &BTree{
		btree: btree.NewBTreeG[*Item](func(a, b *Item) bool {
			return bytes.Compare(a.key, b.key) == -1
		}),
	}
}

// Find 从内存索引中获取key对应的record
func (bt *BTree) Find(key []byte) (*Record, bool) {
	item, ok := bt.btree.Get(&Item{key: key})
	if ok {
		return item.record, ok
	}
	return nil, ok
}

// Insert 将item数据插入内存索引中
// item数据由key和record组成
func (bt *BTree) Insert(record *Record) bool {
	_, replaced := bt.btree.Set(&Item{key: record.Key, record: record})
	return replaced
}

// InsertRecord 将Record数据插入内存索引中
// 提供给list数据结构使用的方法
func (bt *BTree) InsertRecord(key []byte, record *Record) bool {
	_, replaced := bt.btree.Set(&Item{key: key, record: record})
	return replaced
}

// Delete 从内存索引中删除key对应的记录
func (bt *BTree) Delete(key []byte) bool {
	_, deleted := bt.btree.Delete(&Item{key: key})
	return deleted
}

// All 获取所有record数据
func (bt *BTree) All() []*Record {
	items := bt.btree.Items()

	records := make([]*Record, len(items))
	for i, item := range items {
		records[i] = item.record
	}

	return records
}

// AllItems 获取item数据
// 提供给list数据结构使用的方法
func (bt *BTree) AllItems() []*Item {
	items := bt.btree.Items()
	return items
}

// Range 范围扫描 返回满足范围要求的record数据
// 从start开始排列，小于等于end的key都加入records数组，否则不加入
func (bt *BTree) Range(start, end []byte) []*Record {
	records := make([]*Record, 0)

	bt.btree.Ascend(&Item{key: start}, func(item *Item) bool {
		if bytes.Compare(item.key, end) > 0 {
			return false
		}
		records = append(records, item.record)
		return true
	})

	return records
}

// PrefixScan 前缀扫描 返回满足前缀的records数据
func (bt *BTree) PrefixScan(prefix []byte, offset, limitNum int) []*Record {
	records := make([]*Record, 0)

	// 升序遍历
	// 如果key没有指定前缀，则不加入records数组
	bt.btree.Ascend(&Item{key: prefix}, func(item *Item) bool {
		if !bytes.HasPrefix(item.key, prefix) {
			return false
		}

		// 前offset个满足条件的key不加入records数组
		if offset > 0 {
			offset--
			return true
		}

		records = append(records, item.record)

		limitNum--
		return limitNum != 0
	})

	return records
}

// PrefixSearchScan 前缀+正则扫描 返回满足前缀和正则表达式的records数据
func (bt *BTree) PrefixSearchScan(prefix []byte, reg string, offset, limitNum int) []*Record {
	records := make([]*Record, 0)

	rgx := regexp.MustCompile(reg)

	// 升序遍历
	// 如果key不满足指定前缀则跳过，如果尚未达到偏移位置则跳过，如果不匹配正则表达式则跳过
	bt.btree.Ascend(&Item{key: prefix}, func(item *Item) bool {
		// 是否满足前缀，如果key不满足指定前缀则跳过
		if !bytes.HasPrefix(item.key, prefix) {
			return false
		}

		// 是否达到满足前缀的key的偏移位置，如果尚未达到偏移位置则跳过
		if offset > 0 {
			offset--
			return true
		}

		// 是否匹配正则表达式
		// 首先去掉前缀，然后进行正则匹配，如果不匹配则跳过
		if !rgx.Match(bytes.TrimPrefix(item.key, prefix)) {
			return true
		}

		records = append(records, item.record)

		limitNum--
		return limitNum != 0
	})

	return records
}

// Count 返回BTree的数据大小
func (bt *BTree) Count() int {
	return bt.btree.Len()
}

// PopMin 弹出BTree中最小的Item
// todo 什么场景会用到这种功能？
func (bt *BTree) PopMin() (*Item, bool) {
	return bt.btree.PopMin()
}

// PopMax 弹出BTree中最大的Item
func (bt *BTree) PopMax() (*Item, bool) {
	return bt.btree.PopMax()
}

// Min 返回Btree中最小的Item
func (bt *BTree) Min() (*Item, bool) {
	return bt.btree.Min()
}

// Max 返回Btree中最大的Item
func (bt *BTree) Max() (*Item, bool) {
	return bt.btree.Max()
}
