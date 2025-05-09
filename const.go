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

import "time"

const (
	// WritingStatus WAL正处于正常写入状态
	WritingStatus = iota + 1
	// SwitchingStatus WAL正处于切换通道状态，数据会可能被暂存到暂存文件
	SwitchingStatus
)

const (
	// DefaultWalFile 基础的WAL文件名称
	DefaultWalFile = "wal.log"
	// DiskFile 基础的暂存文件名称
	DiskFile = "disk.log"
)

const (
	// BufferChannelCount 缓冲通道的数量
	BufferChannelCount = 2
	// MemoryThreshold 触发紧急写入暂存文件的内存阈值
	MemoryThreshold = 0.8
	// MaxRetry 写入通道失败后的重试次数
	MaxRetry = 5
	// BaseRetryDelay 基本的重试延迟
	BaseRetryDelay = 10 * time.Millisecond
)

const (
	// LSNSize (Log Sequence Number)占用的空间，8个字节，可以支持每秒大数据量的写入
	LSNSize = 8
	// BlockDataTypeSize 块中数据的类型占用的空间，2个字节
	BlockDataTypeSize = 2
	// DataLengthSize 数据本身的长度占用的空间，8个字节
	DataLengthSize = 8
	// Crc32Size crc32校验码占用的空间，4个字节
	Crc32Size = 4
	// HeaderSize 头部的长度
	HeaderSize = BlockDataTypeSize + LSNSize + DataLengthSize + Crc32Size

	// LSNOffset LSN的偏移量
	LSNOffset = 0
	// BlockDataTypeOffset BlockDataTypeSize 的偏移量
	BlockDataTypeOffset = LSNOffset + LSNSize
	// DataLengthOffset 数据长度的偏移量
	DataLengthOffset = BlockDataTypeOffset + BlockDataTypeOffset
	// Crc32Offset crc32的偏移量
	Crc32Offset = DataLengthOffset + DataLengthSize
	// ContentOffset 数据内容的偏移量
	ContentOffset = Crc32Offset + Crc32Size
)
