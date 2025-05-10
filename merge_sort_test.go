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
	"math/rand"
	"sort"
	"testing"
)

func TestNewMergeSort(t *testing.T) {
	sources := [][]uint64{
		{1, 5, 9},
		{2, 6, 11},
		{3, 7},
		{1, 2, 10, 11, 13, 19},
		{},
	}

	ms := newMergeSort(sources)
	defer ms.close()

	ms.Run()
}

func TestNewMergeSort_Random(t *testing.T) {
	sources := make([][]uint64, 12)
	for i := 0; i < 10; i++ {
		item := make([]int, 2000)
		for j := range item {
			item[j] = rand.Intn(1000)
		}
		sort.Ints(item)

		sourceItem := make([]uint64, 2000)
		for j, val := range item {
			sourceItem[j] = uint64(val)
		}
		sources[i] = sourceItem
	}

	ms := newMergeSort(sources)
	defer ms.close()

	ms.Run()
}

func BenchmarkMergeSort_Run(b *testing.B) {
	sources := make([][]uint64, 12)
	for i := 0; i < 10; i++ {
		item := make([]int, 1000)
		for j := range item {
			item[j] = rand.Intn(1000)
		}
		sort.Ints(item)

		sourceItem := make([]uint64, 1000)
		for j, val := range item {
			sourceItem[j] = uint64(val)
		}
		sources[i] = sourceItem
	}

	ms := newMergeSort(sources)
	defer ms.close()

	for n := 0; n < b.N; n++ {
		ms.Run()
	}
}
