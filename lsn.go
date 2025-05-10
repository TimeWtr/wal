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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	lsnOnce      sync.Once
	closeLsnOnce sync.Once
	lsnManager   *LSNManager
)

const MaxCounter = 5

type Generator interface {
	// Next 获取下一个LSN
	Next() (LSN, error)
	// Current 获取当前LSN
	Current() LSN
	// ResetEpoch 重置时间基线
	ResetEpoch(epoch time.Time)
	// Close 关闭
	Close()
}

type lsnState struct {
	// 上次生成LSN的时间
	lastTime int64
	// 计数器
	counter uint8
	// 当前的LSN
	lsn LSN
}

// lsnGenerator 基于epoch基准时间线来进行计算高位并生成完整的LSN，
type lsnGenerator struct {
	// 数据中心
	centerID uint8
	// 机器码
	machineID uint8
	// 时间线
	timeline uint8
	// 时间基线
	epoch atomic.Int64
	// 状态
	state atomic.Value
	// 缓存时间戳，防止高并发场景下频繁调用内核获取时间造成的时间占用和CPU占用
	now atomic.Int64
	// 关闭信号
	sig chan struct{}
}

// TODO 添加指标采集
func newLsnGenerator(centerID, machineID, timeline uint8, epoch time.Time) Generator {
	gen := &lsnGenerator{
		centerID:  centerID,
		machineID: machineID,
		timeline:  timeline,
		sig:       make(chan struct{}),
	}

	gen.epoch.Store(epoch.UnixMilli())
	state := &lsnState{}
	gen.state.Store(state)

	// 性能优化，防止频繁的获取当前时间窗口
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		for {
			select {
			case <-gen.sig:
				return
			case <-ticker.C:
				gen.now.Store(time.Now().UnixMilli() - gen.epoch.Load())
			}
		}
	}()

	return gen
}

func (l *lsnGenerator) Next() (LSN, error) {
	counter := uint8(0)
	for {
		select {
		case <-l.sig:
			return LSN{}, errors.New("lsn generator closed")
		default:
		}

		counter++
		oldState := l.state.Load().(*lsnState)
		now := l.now.Load()

		// 时钟回拨问题
		if now < oldState.lastTime {
			if delta := oldState.lastTime - now; delta < 5_000 {
				runtime.Gosched()
				continue
			} else {
				return oldState.lsn, fmt.Errorf("critical clock rollback %dms", delta)
			}
		}

		// 处理计时器溢出的问题
		var newCounter uint8
		if now == oldState.lastTime {
			if oldState.counter >= 0x7F {
				return oldState.lsn, fmt.Errorf("counter overflow")
			}

			newCounter = oldState.counter + 1
		} else {
			newCounter = 0
		}

		// 构建新的LSN
		newState := &lsnState{
			lastTime: now,
			counter:  newCounter,
			lsn: LSN{
				Timeline:  l.timeline,
				CenterID:  l.centerID,
				MachineID: l.machineID,
				Timestamp: now,
				counter:   newCounter,
			},
		}

		if l.state.CompareAndSwap(oldState, newState) {
			return newState.lsn, nil
		}

		if counter > MaxCounter {
			runtime.Gosched()
		}
	}
}

func (l *lsnGenerator) Current() LSN {
	return l.state.Load().(*lsnState).lsn
}

func (l *lsnGenerator) ResetEpoch(epoch time.Time) {
	l.epoch.Store(epoch.UnixMilli())
}

func (l *lsnGenerator) Close() {
	close(l.sig)
}

type Storage interface {
	// Save 持久化到当前的段文件
	Save(lsn LSN) error
	// Load 加载最近的LSN
	Load() (LSN, error)
	// SaveAndSync 保存并强制刷盘
	SaveAndSync(lsn LSN) error
	// Sync 强制刷盘
	Sync() error
	// Close 关闭信号
	Close()
}

// FileStorage 本地文件存储当前的LSN进度，即最新的LSN和毫秒时间戳
type FileStorage struct {
	f    *os.File
	lock sync.Mutex
}

func newFileStorage(dir string) (Storage, error) {
	f, err := os.OpenFile(filepath.Join(dir, "wal.lsn"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	return &FileStorage{
		f:    f,
		lock: sync.Mutex{},
	}, nil
}

func (f *FileStorage) Save(lsn LSN) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	encoded := make([]byte, 16)
	binary.BigEndian.PutUint64(encoded[:8], encode(lsn))
	binary.BigEndian.PutUint64(encoded[8:], uint64(time.Now().UnixMilli()))

	_, err := f.f.WriteAt(encoded, 0)

	return err
}

func (f *FileStorage) Load() (LSN, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	var lsn LSN
	bs := make([]byte, 16)
	_, err := f.f.ReadAt(bs, 0)
	if err != nil {
		if errors.Is(err, io.EOF) {
			// 初始化状态
			return lsn, nil
		}

		return lsn, fmt.Errorf("read LSN failed: %v", err)
	}

	// 校验时间戳
	storedTime := binary.BigEndian.Uint64(bs[8:])
	if time.Now().Sub(time.UnixMilli(int64(storedTime))) > time.Hour {
		return LSN{}, errors.New("stale LSN detected")
	}

	return decode(binary.BigEndian.Uint64(bs[:8])), nil
}

func (f *FileStorage) SaveAndSync(lsn LSN) error {
	if err := f.Save(lsn); err != nil {
		return err
	}

	return f.Sync()
}

func (f *FileStorage) Sync() error {
	return f.f.Sync()
}

func (f *FileStorage) Close() {
	_ = f.f.Close()
}

// LSN 数据结构设计
// +---------+------------+----------+---------------------+-----------+
// | 6 bits  ｜5 bits     | 5 bits   | 41 bits             | 7 bits   |
// | Timeline| DataCenter | Instance | Timestamp           | Counter   |
// +---------+------------+----------+---------------------+-----------+
// 这样的设计：
//
//	单实例生成能力
//		每秒容量：128（计数器容量） × 1,000 ms = 128,000 LSN/秒
//		每日容量：128,000 × 86,400秒 = 11,059,200,000 LSN ≈ 110.6亿/天
//	单机房生成能力（32实例）
//		每秒容量：128,000 × 32 = 4,096,000 LSN/秒
//		每日容量：4,096,000 × 86,400 ≈ 353.9亿/天
//	集群生成能力（32机房 × 32实例）
//		每秒容量：128,000 × 32 × 32 = 131,072,000 LSN/秒
//		每日容量：131,072,000 × 86,400 ≈ 11,324亿/天
type LSN struct {
	// 时间线，6位
	Timeline uint8
	// 数据中心，5位
	CenterID uint8
	// 机器ID，5位
	MachineID uint8
	// 时间戳，毫秒级，41位
	Timestamp int64
	// 计数器，7位
	counter uint8
}

func encode(l LSN) uint64 {
	return (uint64(l.Timeline&0x3F) << 58) |
		uint64(l.CenterID&0x1F)<<53 |
		uint64(l.MachineID&0x1F)<<48 |
		uint64(l.Timestamp&0xFFFFFFFFFF)<<7 |
		uint64(l.counter&0x7F)
}

func decode(lsn uint64) LSN {
	return LSN{
		Timeline:  uint8(lsn >> 58 & 0x3F),
		CenterID:  uint8(lsn >> 53 & 0x1F),
		MachineID: uint8(lsn >> 48 & 0x1F),
		Timestamp: int64(lsn >> 7 & 0xFFFFFFFFFF),
		counter:   uint8(lsn & 0x7F),
	}
}

// LSNManager LSN的生成管理器
type LSNManager struct {
	g Generator
	s Storage
}

func newLSNManager(g Generator, s Storage) (*LSNManager, error) {
	var err error
	lsnOnce.Do(func() {
		lsnManager = &LSNManager{
			g: g,
			s: s,
		}
	})

	return lsnManager, err
}

// Save 保存LSN数据到PageCache中，还未直接写入磁盘文件
func (lm *LSNManager) Save() error {
	return lm.s.Save(lm.g.Current())
}

func (lm *LSNManager) Load() (LSN, error) {
	return lm.s.Load()
}

func (lm *LSNManager) SaveAndSync() error {
	return lm.s.SaveAndSync(lm.g.Current())
}

// Sync 直接同步刷盘，把操作系统PageCache中的缓存数据强制刷盘到持久化文件中
func (lm *LSNManager) Sync() error {
	return lm.s.Sync()
}

func (lm *LSNManager) Next() (LSN, error) {
	return lm.g.Next()
}

func (lm *LSNManager) Current() LSN {
	return lm.g.Current()
}

func (lm *LSNManager) Close() {
	closeLsnOnce.Do(func() {
		lm.g.Close()
		lm.s.Close()
	})
}
