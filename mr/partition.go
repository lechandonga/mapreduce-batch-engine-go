package mr

import "hash/fnv"

// partitionFor 返回键所属的 reduce 分区。
//
// 只依赖 (key, partitions)，与并发度、机器、Go map 遍历顺序无关：
// 使用 FNV-1a（算法标准、跨进程稳定），空键固定落在 0 号分区。
// 同一个键的所有记录必然进入同一个 reduce 分组。
func partitionFor(key string, partitions int) int {
	if partitions <= 1 {
		return 0
	}
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(partitions))
}
