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
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
)

var pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 1024)
	},
}

const (
	WritingBufferStatus = iota
	ReadingBufferStatus
)

const (
	// MixBufferSize 低级的Buffer容量(16M)
	MixBufferSize = 1024 * 1024 * 16
	// MiddleBufferSize 中级的Buffer容量(64MB)
	MiddleBufferSize = 1024 * 1024 * 64
	// HighBufferSize 高级的Buffer容量(256MB)
	HighBufferSize = 1024 * 1024 * 256
)

const (
	// DefaultBatchReadSize 异步刷盘时每次从缓冲区读取2M的数据
	DefaultBatchReadSize = 1024 * 1024 * 2

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

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Buffer 设计需要偏移量来实现，通过偏移量来判断是否需要切换通道
type Buffer struct {
	// 连续的内存空间
	data []byte
	//填充64字节，隔离data和w
	_ [64]byte
	// 当前正在写的偏移量
	w int64
	// 填充64字节，隔离w和r
	_ [64]byte
	// 当前正在读的偏移量
	r int64
	// 填充64字节隔离r和isFull
	_ [64]byte
	// 是否已满
	isFull atomic.Bool
	// 当前缓冲区状态
	status atomic.Uint32
	// buffer的容量
	capacity int64
	// walFile文件句柄
	wal *os.File
	// 加锁保护
	mu sync.Mutex
	// 单次读取块的大小，默认为1M
	blockSize int64
	// 关闭信号
	closed chan struct{}
}

func newBuffer(capacity int64, wal *os.File, blockSize int64) *Buffer {
	b := &Buffer{
		data:      make([]byte, capacity),
		capacity:  capacity,
		wal:       wal,
		mu:        sync.Mutex{},
		blockSize: blockSize,
		closed:    make(chan struct{}),
	}
	b.isFull.Store(false)
	b.status.Store(WritingBufferStatus)

	return b
}

// free 通道剩余的空间，字节数
func (b *Buffer) free() int64 {
	return b.capacity - b.w
}

func (b *Buffer) full() bool {
	return b.isFull.Load()
}

// reset 重置缓冲区，用于在触发切换时，重置的缓冲区切换为新写入的缓冲区
func (b *Buffer) reset() {
	for i := range b.data {
		b.data[i] = 0
	}
	b.w, b.r = 0, 0
	b.isFull.Store(false)
}

// write 写入数据，还需要传入参数标识是否是完整的数据
func (b *Buffer) write(data []byte, isFull bool) error {
	if b.status.Load() == ReadingBufferStatus {
		return ErrBufferFull
	}

	if b.isFull.Load() {
		return ErrBufferFull
	}

	b.mu.Lock()
	requireSize := int64(HeaderSize + len(data))
	if b.free() < requireSize {
		b.isFull.Store(true)
		b.mu.Unlock()
		return ErrBufferFull
	}
	b.mu.Unlock()

	copy(b.data[b.w:requireSize], data)
	b.w += requireSize

	return nil
}

func (b *Buffer) lsn() uint64 {
	return uint64(rand.Intn(10000))
}

// asyncRead 异步读取，因为是双通道会切换通道，所以当该通道转换为异步刷盘通道读取
// 数据时，不会出现buffer写入的情况，使用原子状态判断来避免多个goroutine同时读和写文件
func (b *Buffer) asyncRead() {
	if b.status.Load() == ReadingBufferStatus {
		return
	}

	b.switchReading()
	for b.r <= b.w && b.w != 0 {
		select {
		case <-b.closed:
			return
		default:
		}

		endOffset := b.r + b.blockSize
		if endOffset >= b.w {
			endOffset = b.w
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

		if err = b.wal.Sync(); err != nil {
			if errors.Is(err, fs.ErrInvalid) {
				_, _ = os.Stderr.WriteString("failed to persist, cause: " + err.Error())
				return
			}
			_ = b.wal.Sync()
			//todo 需要处理刷盘失败问题以及重试可能导致的数据写入重复问题
			fmt.Println("同步失败：", err)
			continue
		}

		b.r = endOffset
	}

	b.reset()
	b.switchWriting()
}

func (b *Buffer) switchReading() {
	b.status.Store(ReadingBufferStatus)
}

func (b *Buffer) switchWriting() {
	b.status.Store(WritingBufferStatus)
}

func (b *Buffer) Close() {
	close(b.closed)
}
