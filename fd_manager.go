package nutsdb

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	// 默认最大文件数
	DefaultMaxFileNums = 256
)

const (
	// 用于判断"打开文件过多的错误"的后缀
	TooManyFileOpenErrSuffix = "too many open files"
)

// fdManager hold a fd cache in memory, it lru based cache.
// fdManager 在内存中持有fd缓存，该缓存是基于LRU的
type fdManager struct {
	// 锁
	lock sync.Mutex
	// 缓存
	// 文件名 -> 文件元数据
	cache map[string]*FdInfo
	// 文件list
	fdList *doubleLinkedList
	// 大小
	size int
	// 进行清理的阈值数
	cleanThresholdNums int
	// 最大文件数
	maxFdNums int
}

// newFdm will return a fdManager object
// newFdm 将会返回一个fdManager实例
func newFdm(maxFdNums int, cleanThreshold float64) (fdm *fdManager) {
	fdm = &fdManager{
		cache:     map[string]*FdInfo{},
		fdList:    initDoubleLinkedList(),
		size:      0,
		maxFdNums: DefaultMaxFileNums,
	}
	// 清理阈值是最大文件数的一半
	fdm.cleanThresholdNums = int(math.Floor(0.5 * float64(fdm.maxFdNums)))
	// 如果传入的最大文件数大于0，则使用它，否则使用默认最大文件数
	if maxFdNums > 0 {
		fdm.maxFdNums = maxFdNums
	}

	// 如果传入的cleanThreshold大于0 并且小于1,则使用它乘以最大文件数得到cleanThresholdNums，否则使用默认的
	if cleanThreshold > 0.0 && cleanThreshold < 1.0 {
		fdm.cleanThresholdNums = int(math.Floor(cleanThreshold * float64(fdm.maxFdNums)))
	}
	return fdm
}

// FdInfo holds base fd info
// FdInfo 持有fd的基础信息
type FdInfo struct {
	// 文件句柄
	fd *os.File
	// 文件路径
	path string
	// fd使用次数
	using uint
	// 指向的下一个文件
	next *FdInfo
	// 指向的上一个文件
	prev *FdInfo
}

// getFd go through this method to get fd.
// getFd 获取fd
func (fdm *fdManager) getFd(path string) (fd *os.File, err error) {
	fdm.lock.Lock()
	defer fdm.lock.Unlock()
	cleanPath := filepath.Clean(path)
	// 尝试从缓存中获取fd
	// 如果缓存为空，则使用文件全路径打开该文件
	//		如果打开正常，则将fd添加进缓存然后返回
	//		如果打开异常，则判断错误是否是TooManyFileOpenErr，如果是则将尝试再次打开
	// 如果缓存不为空，则using++且将该fdInfo移动至list首部，然后返回fd
	if fdInfo := fdm.cache[cleanPath]; fdInfo == nil {
		fd, err = os.OpenFile(cleanPath, os.O_CREATE|os.O_RDWR, 0o644)
		if err == nil {
			// if the numbers of fd in cache larger than the cleanThreshold in config, we will clean useless fd in cache
			// 如果cache中的fd数量大于配置的清理阈值，我们将清理cache中不使用的fd
			// 允许清理失败，清理失败不返回错误
			if fdm.size >= fdm.cleanThresholdNums {
				err = fdm.cleanUselessFd()
			}
			// if the numbers of fd in cache larger than the max numbers of fd in config, we will not add this fd to cache
			// 如果fd的数量大于配置的最大fd数量，我们不会将该fd添加进缓存
			if fdm.size >= fdm.maxFdNums {
				return fd, nil
			}
			// add this fd to cache
			// 将fd添加进缓存
			fdm.addToCache(fd, cleanPath)
			return fd, nil
		} else {
			// determine if there are too many open files, we will first clean useless fd in cache and try open this file again
			// 如果识别到TooManyFileOpenErr，我们首先将清理缓存中不使用的fd，然后再次尝试打开该文件
			if strings.HasSuffix(err.Error(), TooManyFileOpenErrSuffix) {
				// 清理不使用的fd
				cleanErr := fdm.cleanUselessFd()
				// if something wrong in cleanUselessFd, we will return "open too many files" err, because we want user not the main err is that
				// 如果清理不使用的fd时发生错误，我们将会返回原始错误。因为我们不希望用户关注清理fd发生的错误。
				if cleanErr != nil {
					return nil, err
				}
				// try open this file again，if it still returns err, we will show this error to user
				// 再次尝试打开该文件，如果仍然返回错误，我们将会把最新的错误返回给用户
				fd, err = os.OpenFile(cleanPath, os.O_CREATE|os.O_RDWR, 0o644)
				if err != nil {
					return nil, err
				}
				// add to cache if open this file successfully
				// 如果成功打开文件，则将其加入缓存
				fdm.addToCache(fd, cleanPath)
			}
			return fd, err
		}
	} else {
		// 该fd使用次数+1
		fdInfo.using++
		// 将该fdInfo移动至lis首部
		fdm.fdList.moveNodeToFront(fdInfo)
		return fdInfo.fd, nil
	}
}

// addToCache add fd to cache
// addToCache 将fd添加进cache
func (fdm *fdManager) addToCache(fd *os.File, cleanPath string) {
	fdInfo := &FdInfo{
		fd:    fd,
		using: 1,
		path:  cleanPath,
	}
	// 将该fdInfo添加进list
	fdm.fdList.addNode(fdInfo)
	// 文件管理器大小+1
	fdm.size++
	// 将该fdInfo添加进cache
	fdm.cache[cleanPath] = fdInfo
}

// reduceUsing when RWManager object close, it will go through this method let fdm know it return the fd to cache
// reduceUsing 当 RWManager 实例关闭时，它会通过此方法让 fdm 知道它将 fd 返回缓存
func (fdm *fdManager) reduceUsing(path string) {
	fdm.lock.Lock()
	defer fdm.lock.Unlock()
	// 去除路径中的冗余部分，如 . 和 ..，使路径规范化
	cleanPath := filepath.Clean(path)
	// 尝试从缓存中获取节点
	node, isExist := fdm.cache[cleanPath]
	// 如果节点不在缓存中返回崩溃
	if !isExist {
		panic("unexpected the node is not in cache")
	}
	// fd使用次数-1
	node.using--
}

// close means the cache.
// close 将缓存的文件关闭
func (fdm *fdManager) close() error {
	fdm.lock.Lock()
	defer fdm.lock.Unlock()
	// 获取尾节点的前一节点
	node := fdm.fdList.tail.prev
	// 如果节点不是头节点就继续循环
	for node != fdm.fdList.head {
		// 关闭文件
		err := node.fd.Close()
		if err != nil {
			return err
		}
		// 删除缓存
		delete(fdm.cache, node.path)
		// 文件管理器大小-1
		fdm.size--
		// 指向前一节点
		node = node.prev
	}
	// next指针指向尾节点
	fdm.fdList.head.next = fdm.fdList.tail
	// prev指针指向头节点
	fdm.fdList.tail.prev = fdm.fdList.head
	return nil
}

// doubleLinkedList 存储FdInfo的双向链表
type doubleLinkedList struct {
	head *FdInfo
	tail *FdInfo
	size int
}

// initDoubleLinkedList 初始化doubleLinkedList
func initDoubleLinkedList() *doubleLinkedList {
	list := &doubleLinkedList{
		head: &FdInfo{},
		tail: &FdInfo{},
		size: 0,
	}
	list.head.next = list.tail
	list.tail.prev = list.head
	return list
}

func (list *doubleLinkedList) addNode(node *FdInfo) {
	list.head.next.prev = node
	node.next = list.head.next
	list.head.next = node
	node.prev = list.head
	list.size++
}

func (list *doubleLinkedList) removeNode(node *FdInfo) {
	node.prev.next = node.next
	node.next.prev = node.prev
	node.prev = nil
	node.next = nil
}

func (list *doubleLinkedList) moveNodeToFront(node *FdInfo) {
	list.removeNode(node)
	list.addNode(node)
}

func (fdm *fdManager) cleanUselessFd() error {
	cleanNums := fdm.cleanThresholdNums
	node := fdm.fdList.tail.prev
	for node != nil && node != fdm.fdList.head && cleanNums > 0 {
		nextItem := node.prev
		if node.using == 0 {
			fdm.fdList.removeNode(node)
			err := node.fd.Close()
			if err != nil {
				return err
			}
			fdm.size--
			delete(fdm.cache, node.path)
			cleanNums--
		}
		node = nextItem
	}
	return nil
}

func (fdm *fdManager) closeByPath(path string) error {
	fdm.lock.Lock()
	defer fdm.lock.Unlock()
	fdInfo, ok := fdm.cache[path]
	if !ok {
		return nil
	}
	delete(fdm.cache, path)

	fdm.fdList.removeNode(fdInfo)
	return fdInfo.fd.Close()
}
