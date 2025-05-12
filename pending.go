package nutsdb

// EntryStatus represents the Entry status in the current Tx
type EntryStatus = uint8

const (
	// NotFoundEntry means there is no changes for this entry in current Tx
	NotFoundEntry EntryStatus = 0
	// EntryDeleted means this Entry has been deleted in the current Tx
	EntryDeleted EntryStatus = 1
	// EntryUpdated means this Entry has been updated in the current Tx
	EntryUpdated EntryStatus = 2
)

// BucketStatus represents the current status of bucket in current Tx
type BucketStatus = uint8

const (
	// BucketStatusExistAlready means this bucket already exists
	BucketStatusExistAlready = 1
	// BucketStatusDeleted means this bucket is already deleted
	BucketStatusDeleted = 2
	// BucketStatusNew means this bucket is created in current Tx
	BucketStatusNew = 3
	// BucketStatusUpdated means this bucket is updated in current Tx
	BucketStatusUpdated = 4
	// BucketStatusUnknown means this bucket doesn't exist
	BucketStatusUnknown = 5
)

// pendingBucketList the uncommitted bucket changes in this Tx
type pendingBucketList map[Ds]map[BucketName]*Bucket

// pendingEntriesInBTree means the changes Entries in DataStructureBTree in the Tx
type pendingEntriesInBTree map[BucketName]map[string]*Entry

// pendingEntryList the uncommitted Entry changes in this Tx
type pendingEntryList struct {
	entriesInBTree pendingEntriesInBTree
	entries        map[Ds]map[BucketName][]*Entry
	size           int
}

// newPendingEntriesList create a new pendingEntryList object for a Tx
func newPendingEntriesList() *pendingEntryList {
	pending := &pendingEntryList{
		entriesInBTree: map[BucketName]map[string]*Entry{},
		entries:        map[Ds]map[BucketName][]*Entry{},
		size:           0,
	}
	return pending
}

// submitEntry submit an entry into pendingEntryList
// submitEntry 将键值对提交进待写数组
func (pending *pendingEntryList) submitEntry(ds Ds, bucket string, e *Entry) {
	switch ds {
	case DataStructureBTree:
		// 如果桶在BTree中不存在,则新建一个
		if _, exist := pending.entriesInBTree[bucket]; !exist {
			pending.entriesInBTree[bucket] = map[string]*Entry{}
		}
		// 如果Key在桶中不存在则新建一个相关记录
		if _, exist := pending.entriesInBTree[bucket][string(e.Key)]; !exist {
			pending.entriesInBTree[bucket][string(e.Key)] = e
			pending.size++
		}
	default:
		//如果ds数据结构在待写数据中不存在,则新建一个
		if _, exist := pending.entries[ds]; !exist {
			pending.entries[ds] = map[BucketName][]*Entry{}
		}
		// 获取根据ds数据结构+bucket桶名获取数据数据,将数据添加进数据数组中
		entries := pending.entries[ds][bucket]
		entries = append(entries, e)
		pending.entries[ds][bucket] = entries
		pending.size++
	}
}

// rangeBucket input a range handler function f and call it with every bucket in pendingBucketList
func (p pendingBucketList) rangeBucket(f func(bucket *Bucket) error) error {
	for _, bucketsInDs := range p {
		for _, bucket := range bucketsInDs {
			err := f(bucket)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// toList collect all the entries in pendingEntryList to a list.
// toList 将 pendingEntryList 中的所有元素收集到一个列表中。
func (pending *pendingEntryList) toList() []*Entry {
	list := make([]*Entry, 0, pending.size)
	for _, entriesInBucket := range pending.entriesInBTree {
		for _, entry := range entriesInBucket {
			list = append(list, entry)
		}
	}
	for _, entriesInDS := range pending.entries {
		for _, entries := range entriesInDS {
			for _, entry := range entries {
				list = append(list, entry)
			}
		}
	}
	return list
}
