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
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const BaseYear = 2025

var (
	lsnOnce    sync.Once
	lsnManager *LSNManager
)

type Generator interface {
	// Next 获取下一个LSN
	Next() LSN
	// Current 获取当前LSN
	Current() LSN
	// Reset 重置LSN时间线
	Reset(timeline uint32)
}

// lsnGenrator 基于epoch基准时间线来进行计算高位并生成完整的LSN，
type lsnGenrator struct {
	lock sync.Mutex
	// 时间基准线
	epoch time.Time
	// 机器码，10位
	machineID uint16
	// 上次生成LSN的时间
	// 计数器，14位
	counter atomic.Uint32
}

func (l *lsnGenrator) Next() LSN {
	//TODO implement me
	panic("implement me")
}

func (l *lsnGenrator) Current() LSN {
	//TODO implement me
	panic("implement me")
}

func (l *lsnGenrator) Reset(timeline uint8) {
	l.lock.Lock()
	defer l.lock.Unlock()
}

type Storage interface {
	// Save 持久化到当前的段文件
	Save(lsn LSN) error
	// Load 加载最近的LSN
	Load() (LSN, error)
	// Sync 强制刷盘
	Sync() error
}

// LSN 数据结构设计
// +---------+-----------+---------------------+-----------+
// | 8 bits  | 10 bits   | 41 bits             | 12 bits   |
// | Timeline| MachineID | Timestamp           | Counter   |
// +---------+-----------+---------------------+-----------+
// 这样的设计：
//
//		单节点：每秒可以生成16383个LSN，每天可以生成14.15亿
//		集群(1024)：每秒可以生成1,677万个LSN，每天可以生成1.45万亿
//	    理论上可以支撑136年
type LSN struct {
	// 时间线
	Timeline uint8
	// 机器ID，10位
	MachineID uint16
	// 时间戳，秒级，32位
	Timestamp int32
	// 计数器，14位
	couter uint16
}

func encode(l LSN) uint64 {
	return (uint64(l.Timeline) << 56) |
		uint64(l.MachineID)<<46 |
		uint64(l.Timestamp)<<14 |
		uint64(l.couter)
}

func decode(lsn uint64) LSN {
	return LSN{
		Timeline:  uint8(lsn >> 56),
		MachineID: uint16(lsn >> 46 & 0x3FF),
		Timestamp: int32(lsn >> 14 & 0xFFFFFFFF),
		couter:    uint16(lsn >> 14 & 0x3FF),
	}
}

// LSNManager LSN的生成管理器
type LSNManager struct {
	f    *os.File
	g    Generator
	s    Storage
	lock sync.RWMutex
}

func newLSNManager(dir string, g Generator, s Storage) (*LSNManager, error) {
	var err error
	lsnOnce.Do(func() {
		lsnManager = &LSNManager{
			g:    g,
			s:    s,
			lock: sync.RWMutex{},
		}

		if lsnManager.f == nil {
			lsnManager.f, err = os.OpenFile(filepath.Join(dir, "wal.lsn"), os.O_CREATE|os.O_RDWR, 0600)
		}
	})

	return lsnManager, err
}

// Save 保存LSN数据到PageCache中，还未直接写入磁盘文件
func (lm *LSNManager) Save(lsn LSN) error {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	bs, err := json.Marshal(lsn)
	if err != nil {
		return err
	}

	_, err = lm.f.WriteAt(bs, 0)
	return err
}

func (lm *LSNManager) Load() (LSN, error) {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	var lsn LSN
	var bs []byte
	_, err := lm.f.ReadAt(bs, 0)
	if err != nil {
		return lsn, err
	}

	return lsn, json.Unmarshal(bs, &lsn)
}

// Sync 直接同步刷盘，把操作系统PageCache中的缓存数据强制刷盘到持久化文件中
func (lm *LSNManager) Sync() error {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	return lm.f.Sync()
}

func (lm *LSNManager) Next() LSN {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	return lm.g.Next()
}

func (lm *LSNManager) Current() LSN {
	lm.lock.RLock()
	defer lm.lock.RUnlock()

	return lm.g.Current()
}

func (lm *LSNManager) Reset(timeline uint32) {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	lm.g.Reset(timeline)
}

func (lm *LSNManager) close() {
	_ = lm.f.Close()
}
