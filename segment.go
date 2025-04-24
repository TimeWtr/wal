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

import "os"

// Segment WAL段的定义
type Segment struct {
	// 段文件编号
	sid uint32
	// 段文件句柄
	f os.File
	// 当前已写入的偏移量
	offset int64
	// 是否已经归档
	flushed bool
	// 段文件路径
	path string
	// 开始的LSN
	startLSN uint64
}
