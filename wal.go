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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	WritingStatus = iota + 1
	SwitchingStatus
)

const BufferChannelCount = 2
const DefaultWalFile = "wal.log"

// WAL wal机制的数据结构，采用双通道+异步刷盘机制，双通道分为当前正在写入的通道和异步刷盘的通道，
// 当达到切换通道条件时，写入通道和刷盘通道实现自动无缝切换，异步刷盘->写入，写入->异步刷盘。
// 条件：
//  1. 写入通道达到阈值(百分比/实际字节容量)
//  2. 日志条数达到阈值
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
	// 加锁保护
	lock *sync.RWMutex
	// 关闭通知通道
	closeCh chan struct{}
	// 定时ticker
	t *time.Ticker
	// WAL的状态：切换通道和写入中
	s atomic.Uint32
	// Wal文件句柄
	walFile *os.File
}

//nolint:deadcode,unused
func NewWAL(capacity int64, dir string) (*WAL, error) {
	walFile, err := os.OpenFile(filepath.Join(dir, DefaultWalFile), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	buffers := make([]*Buffer, BufferChannelCount)
	buffers[0] = newBuffer(capacity, walFile)
	buffers[1] = newBuffer(capacity, walFile)

	w := &WAL{
		buffers: []*Buffer{},
		walFile: walFile,
		s:       atomic.Uint32{},
	}
	w.s.Store(WritingStatus)

	return w, nil
}

func (w *WAL) Write(p []byte) error {
	if w.s.Load() == SwitchingStatus {
		return ErrBufferSwitching
	}

	activeBufferIdx := w.activeBufferIdx.Load()
	err := w.buffers[activeBufferIdx].write(p, true)
	if err != nil && !errors.Is(err, ErrBufferFull) {
		return err
	}

	// 通道已满，需要切换通道
	w.switchStatus()
	return w.buffers[w.activeBufferIdx.Load()].write(p, true)
}

// asyncWorker 用于定时切换实时写入通道和异步刷盘通道，并启动异步任务执行刷盘
func (w *WAL) asyncWorker() {
	for range w.t.C {
		w.switchStatus()
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

	w.s.Store(SwitchingStatus)
	activeBufferIdx := w.activeBufferIdx.Load()
	w.activeBufferIdx.Store(1 - activeBufferIdx)
	go w.buffers[activeBufferIdx].asyncRead()
	w.s.Store(WritingStatus)
}
