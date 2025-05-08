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
	"github.com/chen3feng/stl4go"
	"sync"
)

type DataPoint struct {
	// Log Sequence Number
	LSN int64
	// 需要写入的数据，包括已经加入的Header头部
	Data []byte
}

const ChannelSize = 1024

type Buffer2 struct {
	// 接收写入的缓冲通道，用于接收写入的数据，由于并发写入问题，可能导致LSN数据全局递增有序，但是
	// 在通道内部乱序，以及通道容量满了之后，出现LSN空洞问题，即出现两个LSN之间不是连续的
	recvq chan DataPoint
	// 异步刷盘期间轮询发送有序跳表数据的通道
	readq chan DataPoint
	// 基于LSN的有序跳表
	sk     *stl4go.SkipList[int64, []byte]
	minLsn int64
	status int8
	lock   sync.RWMutex
	pool   *sync.Pool
}

func NewBuffer2() *Buffer2 {
	buf := &Buffer2{
		recvq:  make(chan DataPoint, ChannelSize),
		readq:  make(chan DataPoint, ChannelSize),
		sk:     stl4go.NewSkipList[int64, []byte](),
		status: WritingBufferStatus,
		pool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 4068)
			},
		},
	}

	go buf.asyncWorker()

	return buf
}

func (b *Buffer2) asyncWorker() {
	for {
		select {
		case data, ok := <-b.recvq:
			if !ok {
				return
			}

			cl := b.pool.Get().([]byte)
			if cap(cl) < len(data.Data) {
				cl = make([]byte, len(data.Data))
			}
			copy(cl, data.Data)
			b.lock.Lock()
			b.sk.Insert(data.LSN, cl)
			b.lock.Unlock()
			cl = nil
			b.pool.Put(cl)
		default:
		}
	}
}

// write 往buffer中写入数据，如果写入成功，则判断是否是连续的LSN，即是否存在LSN空洞
// 如果写入失败，说明通道已经满了，需要切换状态为异步读取，停止接收新的数据
func (b *Buffer2) write(p DataPoint) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.status == ReadingBufferStatus {
		return ErrBufferFull
	}

	select {
	case b.recvq <- p:
		b.updateMinLsn(p.LSN)
		if len(b.recvq) == ChannelSize {
			b.status = ReadingBufferStatus
		}

		return nil
	default:
		return ErrBufferFull
	}
}

// updateMinLsn 更新最小LSN
func (b *Buffer2) updateMinLsn(lsn int64) {
	if b.minLsn+1 != lsn {
		return
	}

	b.minLsn = lsn
}

// ReadChannel 获取一个可以读取有序LSN及数据的只读通道，当通道中没有数据时，会close掉
// 如果消费者消费较慢，会等待有容量继续发送数据。当开始读取的时候，通道已经切换完成，即
// 活跃通道切换为异步只读同步通道，不再接收写的操作，所以是通道内的跳表是并发安全的
func (b *Buffer2) ReadChannel() <-chan DataPoint {
	go func() {
		b.sk.ForEach(func(i int64, bytes []byte) {
			b.readq <- DataPoint{
				LSN:  i,
				Data: bytes,
			}
			b.sk.Remove(i)
		})

		close(b.readq)
	}()

	return b.recvq
}

// exists 判断指定的LSN是否存在，存在则返回数据，这个方法只会用在归并排序是出现LSN
// 缺失空洞需要回查当前活跃通道中是否存在缺失的LSN场景。
func (b *Buffer2) exists(lsn int64) (bool, *[]byte) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	v := b.sk.Find(lsn)
	if v == nil {
		return false, nil
	}

	return true, v
}

// reset 重置缓冲区
func (b *Buffer2) reset() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.minLsn = 0
	b.sk = stl4go.NewSkipList[int64, []byte]()
	b.status = WritingBufferStatus
	b.readq = make(chan DataPoint, ChannelSize)
}

func (b *Buffer2) close() {
	close(b.recvq)
}
