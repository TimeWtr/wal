// Copyright 2025 TimeWtr
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
)

//nolint:deadcode,unused
const (
	// MixBufferSize 低级的Buffer容量(32M)
	MixBufferSize = 1024 * 1024 * 16
	// MiddleBufferSize 中级的Buffer容量(64MB)
	MiddleBufferSize = 1024 * 1024 * 64
	// HighBufferSize 高级的Buffer容量(256MB)
	HighBufferSize = 1024 * 1024 * 256
)

const (
	// BatchReadSize 异步刷盘时每次从缓冲区读取1M的数据
	BatchReadSize = 1024 * 1024
	// 测试用的读取块大小
	_testBatchReadSize = 10
)

type BlockDataType uint16

const (
	// BlockTypeFull 标识当前块包括了是一条完整的数据
	BlockTypeFull BlockDataType = iota + 1
	// BlockTypeHeader 标识当前块只包括了数据的头原数据信息
	BlockTypeHeader
	// SegTypeMiddle 标识当前块只包括了内容的中间部分，这个涉及到一条长数据通过多个块传输
	SegTypeMiddle
	// SegTypeEnd 标识当前块包括的是内容的最后结尾部分
	SegTypeEnd
)

func (b BlockDataType) uint16() uint16 {
	return uint16(b)
}

const (
	// BlockDataTypeSize 块中数据的类型占用的空间，2个字节
	BlockDataTypeSize = 2
	// LSN (Log Sequence Number)占用的空间，8个字节，可以支持每秒大数据量的写入
	LSN = 8
	// DataLengthSize 数据本身的长度占用的空间，8个字节
	DataLengthSize = 8
	// Crc32Size crc32校验码占用的空间，4个字节
	Crc32Size = 4
	// HeaderSize 头部的长度
	HeaderSize = BlockDataTypeSize + LSN + DataLengthSize + Crc32Size

	// BlockDataTypeOffset BlockDataTypeSize 的偏移量
	BlockDataTypeOffset = 0
	// LSNOffset LSN的偏移量
	LSNOffset = BlockDataTypeOffset + BlockDataTypeSize
	// DataLengthOffset 数据长度的偏移量
	DataLengthOffset = LSNOffset + LSN
	// Crc32Offset crc32的偏移量
	Crc32Offset = DataLengthOffset + DataLengthSize
	// ContentOffset 数据内容的偏移量
	ContentOffset = Crc32Offset + Crc32Size
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Buffer 设计需要偏移量来实现，通过偏移量来判断是否需要切换通道
type Buffer struct {
	// 连续的内存空间
	data []byte
	//填充64字节，隔离data和w
	_ [64]byte
	// 当前正在写的偏移量
	w int64
	// 下次写入的偏移量
	nextW int64
	// 填充64字节，隔离w和r
	_ [64]byte
	// 当前正在读的偏移量
	r int64
	// 填充64字节隔离r和isFull
	_ [64]byte
	// 是否已满
	isFull bool
	// buffer的容量
	capacity int64
	// walFile文件句柄
	wal *os.File
	// 加锁保护
	mu sync.Mutex
	// 轮询时间间隔

}

func newBuffer(capacity int64, wal *os.File) *Buffer {
	b := &Buffer{
		data:     make([]byte, capacity),
		capacity: capacity,
		wal:      wal,
		mu:       sync.Mutex{},
	}

	return b
}

// free 通道剩余的空间，字节数
func (b *Buffer) free() int64 {
	return b.capacity - b.nextW
}

// reset 重置缓冲区，用于在触发切换时，重置的缓冲区切换为新写入的缓冲区
func (b *Buffer) reset() {
	b.data = b.data[:0]
	b.w, b.r, b.nextW = 0, 0, 0
	b.isFull = false
}

// write 写入数据，还需要传入参数标识是否是完整的数据
func (b *Buffer) write(data []byte, isFull bool) error {
	b.mu.Lock()
	if b.isFull {
		b.mu.Unlock()
		return ErrBufferFull
	}

	requireSize := int64(HeaderSize + len(data))
	if b.capacity-b.nextW < requireSize {
		b.isFull = true
		b.mu.Unlock()
		return ErrBufferFull
	}

	startOffset := b.nextW
	b.nextW = b.nextW + requireSize
	b.mu.Unlock()

	if isFull {
		binary.BigEndian.PutUint16(b.data[startOffset+BlockDataTypeOffset:startOffset+LSNOffset], BlockTypeFull.uint16())
	} else {
		binary.BigEndian.PutUint16(b.data[startOffset+BlockDataTypeOffset:startOffset+startOffset+LSNOffset], BlockTypeHeader.uint16())
	}

	binary.BigEndian.PutUint64(b.data[startOffset+LSNOffset:startOffset+DataLengthOffset], b.lsn())

	binary.BigEndian.PutUint64(b.data[startOffset+DataLengthOffset:startOffset+Crc32Offset], uint64(len(data)))

	checkSum := crc32.Checksum(data, crcTable)
	binary.BigEndian.PutUint32(b.data[startOffset+Crc32Offset:startOffset+ContentOffset], checkSum)

	copy(b.data[startOffset+ContentOffset:], data)
	b.w += HeaderSize

	return nil
}

func (b *Buffer) lsn() uint64 {
	return 0
}

// asyncRead 异步读取，因为是双通道会切换通道，所以当该通道转换为异步刷盘通道读取
// 数据时，不会出现写入的情况，也就是没有并发安全问题。
func (b *Buffer) asyncRead() {
	for b.r <= b.nextW {
		endOffset := b.r + BatchReadSize
		if endOffset >= b.nextW {
			endOffset = b.nextW
		}
		data := b.data[b.r:endOffset]
		if len(data) == 0 {
			break
		}
		_, err := b.wal.Write(data)
		if err != nil {
			// 失败了之后继续读
			fmt.Println("写入失败：", err)
			continue
		}

		b.r = endOffset
	}
	b.isFull = false
	b.reset()
}
