package mr

import "hash/crc32"

var crcTable = crc32.MakeTable(crc32.IEEE)

func crcIEEE(b []byte) uint32 { return crc32.Checksum(b, crcTable) }
