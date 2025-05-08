package nutsdb

import "reflect"

// GetDiskSizeFromSingleObject 从单一结构体中获取所需的磁盘大小
func GetDiskSizeFromSingleObject(obj interface{}) int64 {
	typ := reflect.TypeOf(obj)
	fields := reflect.VisibleFields(typ)
	if len(fields) == 0 {
		return 0
	}
	var size int64 = 0
	for _, field := range fields {
		// Currently, we only use the unsigned value type for our metadata.go. That's reasonable for us.
		// Because it's not possible to use negative value mark the size of data.
		// But if you want to make it more flexible, please help yourself.
		// 实际上，我们的元数据只使用无符号值。因为不可能使用负值代表数据。
		// 但是如果你想要变得更加灵活，请自便。
		switch field.Type.Kind() {
		case reflect.Uint8:
			size += 1
		case reflect.Uint16:
			size += 2
		case reflect.Uint32:
			size += 4
		case reflect.Uint64:
			size += 8
		}
	}
	return size
}
