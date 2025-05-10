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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	lsnOnce    sync.Once
	lsnManager *LSNManager
)

type Generator interface {
	// Next 获取下一个LSN
	Next() (LSN, error)
	// Current 获取当前LSN
	Current() LSN
}

// lsnGenerator 基于epoch基准时间线来进行计算高位并生成完整的LSN，
type lsnGenerator struct {
	lock sync.RWMutex
	// 机器码
	machineID uint16
	// 时间线
	timeline uint8
	// 上次生成LSN的时间
	lastTime int64
	// 计数器
	counter uint8
	// 当前的LSN
	lsn LSN
}

func newLsnGenerator(machineID uint16, timeline uint8) Generator {
	return &lsnGenerator{
		lock:      sync.RWMutex{},
		machineID: machineID,
		timeline:  timeline,
	}
}

func (l *lsnGenerator) Next() (LSN, error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	now := time.Now().UnixMilli()
	if now < l.lastTime {
		// 出现了时钟回拨问题
		delta := l.lastTime - now
		if delta < 5_000 {
			time.Sleep(time.Duration(delta) * time.Millisecond)
			now = time.Now().UnixMilli()
		} else {
			return l.lsn, fmt.Errorf("critical clock rollback %dms", delta)
		}
	}

	// 同一时间窗口内超过了最大数量限制
	if now == l.lastTime && l.counter >= 0x7F {
		return l.lsn, fmt.Errorf("lsn generator counter too big")
	}

	// 新的时间窗口
	if now > l.lastTime {
		l.lastTime = now
		l.counter = 0
	}

	l.counter++
	l.lsn = LSN{
		Timeline:  l.timeline,
		MachineID: l.machineID,
		Timestamp: now,
		couter:    l.counter,
	}

	return l.lsn, nil
}

func (l *lsnGenerator) Current() LSN {
	l.lock.RLock()
	defer l.lock.RUnlock()

	return l.lsn
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
// | 6 bits  | 10 bits   | 41 bits             | 7 bits   |
// | Timeline| MachineID | Timestamp           | Counter   |
// +---------+-----------+---------------------+-----------+
// 这样的设计：
//
//		单节点：每秒可以生成128000个LSN，每天可以生成110.6亿
//		集群(1024)：每秒可以生个131,072,000个LSN，每天可以生成11,324亿
//	    理论上可以支撑69年
type LSN struct {
	// 时间线，6位
	Timeline uint8
	// 机器ID，10位
	MachineID uint16
	// 时间戳，毫秒级，41位
	Timestamp int64
	// 计数器，7位
	couter uint8
}

func encode(l LSN) uint64 {
	return (uint64(l.Timeline&0x3F) << 58) |
		uint64(l.MachineID&0x3FF)<<48 |
		uint64(l.Timestamp&0xFFFFFFFFFF)<<7 |
		uint64(l.couter&0x7F)
}

func decode(lsn uint64) LSN {
	return LSN{
		Timeline:  uint8(lsn >> 58 & 0x3F),
		MachineID: uint16(lsn >> 48 & 0x3FF),
		Timestamp: int64(lsn >> 7 & 0xFFFFFFFFFF),
		couter:    uint8(lsn & 0x7F),
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

func (lm *LSNManager) Next() (LSN, error) {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	return lm.g.Next()
}

func (lm *LSNManager) Current() LSN {
	lm.lock.RLock()
	defer lm.lock.RUnlock()

	return lm.g.Current()
}

func (lm *LSNManager) close() {
	_ = lm.f.Close()
}
