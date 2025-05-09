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
	"fmt"
	gl "github.com/bboreham/go-loser"
	"math"
)

type list struct {
	l   []uint64
	cur uint64
}

func newList(l ...uint64) *list {
	return &list{l: l}
}

func (it *list) At() uint64 {
	return it.cur
}

func (it *list) Next() bool {
	if len(it.l) > 0 {
		it.cur = it.l[0]
		it.l = it.l[1:]
		return true
	}
	it.cur = 0
	return false
}

func (it *list) Seek(val uint64) bool {
	for it.cur < val && len(it.l) > 0 {
		it.cur = it.l[0]
		it.l = it.l[1:]
	}
	return len(it.l) > 0
}

type mergeSort struct {
	tree    *gl.Tree[uint64, *list]
	sources []*list
}

func newMergeSort(sources [][]uint64) *mergeSort {
	ms := &mergeSort{}
	ms.sources = make([]*list, len(sources))
	for i := 0; i < len(sources); i++ {
		ms.sources[i] = newList(sources[i]...)
	}

	ms.tree = gl.New[uint64](ms.sources, math.MaxUint64)
	return ms
}

func (ms *mergeSort) Run() {
	for ms.tree.Next() {
		fmt.Println("at: ", ms.tree.At())
		//todo 获取到当前最小的LSN，根据LSN获取到完整的数据，写入到WAL文件中
	}
}

func (ms *mergeSort) close() {
	ms.tree.Close()
}

func (ms *mergeSort) less(a, b uint64) bool { return a < b }
