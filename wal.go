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
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// WAL wal机制的数据结构，采用双通道+异步刷盘机制，双通道分为当前正在写入的通道和异步刷盘的通道，
// 当达到切换通道条件时，写入通道和刷盘通道实现自动无缝切换，异步刷盘->写入，写入->异步刷盘。
// 条件：
//  1. 写入通道达到阈值(实际字节容量)
//  2. 定时任务轮询切换通道，减少因出现通道长时间未触发阈值无法进行通道切换，导致出现通道内数据丢失风险
type WAL struct {
	// 双缓冲通道，默认开始时下标为0的buffer为实时写入buffer，当达到切换通道时切换为异步刷盘通道
	buffers []*Buffer
	// 单个分段文件的容量大小，默认为100MB
	segmentFileSize int64
	// 所有归档的分段信息
	segments []*Segment
	// 当前活跃的分段信息
	activeSegments *Segment
	// 当前分段的下标，确定当前需要写入哪个分段WAL文件
	idx int64
	// 活跃通道下标
	activeBufferIdx atomic.Uint32
	// 关闭通知通道
	closeCh chan struct{}
	// 定时ticker
	t *time.Ticker
	// WAL的状态：切换通道和写入中
	s atomic.Uint32
	// Wal文件句柄
	walFile *os.File
	// 关闭通知信号
	closed chan struct{}
	// 切换通道中间状态时未写入的数据，阻塞通道，只在通道切换时才会写入
	pending chan [][]byte
	// 数据暂存文件，通道写入后WAL写入前的中间暂存状态的文件
	diskFile *os.File
	// 暂存文件的写入偏移量
	spoolOffset int64
	// 内存池复用内存
	memPool sync.Pool
	// 最近一次刷盘的时间
	lastFlushTime time.Time
	// 内存获取接口
	m Memory
	// 通道切换锁
	switchMu sync.Mutex
}

func NewWAL(capacity int64, dir string, blockSize int64, interval time.Duration) (*WAL, error) {
	walFile, err := os.OpenFile(filepath.Join(dir, DefaultWalFile), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	buffers := make([]*Buffer, BufferChannelCount)
	buffers[0] = newBuffer(capacity, walFile, blockSize)
	buffers[1] = newBuffer(capacity, walFile, blockSize)

	w := &WAL{
		buffers:  buffers,
		walFile:  walFile,
		s:        atomic.Uint32{},
		closed:   make(chan struct{}),
		t:        time.NewTicker(interval),
		switchMu: sync.Mutex{},
		m:        NewMemoryMonitor(),
	}
	w.s.Store(WritingStatus)
	go w.asyncWorker()

	return w, nil
}

// Write 写入数据，会出现双通道切换的问题，必须要保证通道的切换不影响当前数据的写入
// 调用方不关心是否进行通道切换，只关心数据写入问题
// 流程如下：
// 1. 给数据添加元数据头信息：LSNSize、数据类型、数据长度、CRC32校验码
// 2. 获取当前机器的内存占用比例，判断是否大于阈值，大于则跳过缓冲区，立即写入到暂存文件
// 3. 如果正常范围内，则尝试写入到缓冲区通道，重试次数为8次
// 4. 如果出现重试8次后仍然没有写入成功，则立即写入到暂存文件
// 暂存文件的存在以及重试的逻辑会导致原本有序的数据变成了无序，写入的逻辑无需关心顺序问题，
// 有专门的程序来根据LSN的全局单调递增的特性，使用归并排序+LSM树来抽象出索引并生成LSN有序
// 的数据写入到WAL文件，保证最终的有序性。
func (w *WAL) Write(p []byte, isFull bool) error {
	percent, err := w.m.memory()
	if err != nil {
		return err
	}

	dataWithHeader := w.attachMetadataHeader(p, isFull)
	if percent > MemoryThreshold {
		return w.emergencyFlush(dataWithHeader)
	}

	for i := 0; i < MaxRetry; i++ {
		if err = w.directWrite(dataWithHeader); err == nil {
			return nil
		}

		// 重试加入随机抖动，防止出现鲸群效应
		jitter := time.Duration(rand.Intn(MaxRetry)) * time.Millisecond
		retry := BaseRetryDelay*(1<<i) + jitter
		time.Sleep(retry)
	}

	return w.emergencyFlush(dataWithHeader)
}

// directWrite 通道状态为写入中，可以直接将数据写入通道
func (w *WAL) directWrite(p []byte) error {
	return nil
}

// asyncWorker 用于定时切换实时写入通道和异步刷盘通道，并启动异步任务执行刷盘
func (w *WAL) asyncWorker() {
	for {
		select {
		case <-w.closed:
			return
		case <-w.t.C:
			w.switchStatus()
		}
	}
}

// switchStatus 切换状态并开启异步任务
// 1. 切换通道状态为转换中，完全拒绝新的数据写入
// 2. 切换当前活跃通道为异步刷新通道，当前活跃通道下标指向另一个通道
// 3. 开启异步任务执行刷盘
// 4. 切换通道状态为写入中，开始接收新的数据写入
func (w *WAL) switchStatus() {
	if w.s.Load() == SwitchingStatus {
		return
	}

	_, _ = os.Stdout.WriteString(
		fmt.Sprintf("Switching Channel, current channel index: %d\n",
			w.activeBufferIdx.Load()))
	w.s.Store(SwitchingStatus)
	activeBufferIdx := w.activeBufferIdx.Load()
	w.activeBufferIdx.Store(1 - activeBufferIdx)
	go w.buffers[activeBufferIdx].asyncRead()

	w.s.Store(WritingStatus)
}

// Close 关闭WAL
func (w *WAL) Close() {
	w.t.Stop()
	close(w.closed)
}

// emergencyFlush 处理当内存占用过高等紧急情况时，直接写入暂存磁盘文件，防止数据丢失
func (w *WAL) emergencyFlush(p []byte) error {
	w.switchMu.Lock()
	defer w.switchMu.Unlock()

	_, err := w.diskFile.WriteAt(p, w.spoolOffset)
	if err == nil {
		w.spoolOffset += int64(len(p))
	}

	return err
}

// attachMetadataHeader 为每一条数据添加元数据信息，包括：LSNSize、是否完整、数据长度、CRC32校验码
func (w *WAL) attachMetadataHeader(p []byte, isFull bool) []byte {
	data := make([]byte, len(p)+HeaderSize)
	binary.BigEndian.PutUint64(data[LSNOffset:BlockDataTypeOffset], w.lsn())

	if isFull {
		binary.BigEndian.PutUint16(data[BlockDataTypeOffset:DataLengthOffset], BlockTypeFull.uint16())
	} else {
		binary.BigEndian.PutUint16(data[BlockDataTypeOffset:DataLengthOffset], BlockTypeHeader.uint16())
	}

	binary.BigEndian.PutUint64(data[DataLengthOffset:Crc32Offset], uint64(len(data)))

	checkSum := crc32.Checksum(data, crcTable)
	binary.BigEndian.PutUint32(data[Crc32Offset:ContentOffset], checkSum)

	copy(data[ContentOffset:], data)

	return data
}

func (w *WAL) lsn() uint64 {
	return 0
}
