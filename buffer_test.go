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
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"
)

func TestNewBuffer(t *testing.T) {
	walFile, err := os.OpenFile("./logs/wal.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	assert.Nil(t, err)
	buf := newBuffer(1024, walFile, DefaultBatchReadSize)
	defer buf.reset()

	closeCh := make(chan struct{})
	ticker := time.NewTicker(time.Second)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()

		for i := 0; i < 100; i++ {
			msg := []byte(fmt.Sprintf("this is a test message, number: %d\n", i))
			isFull := false
			if i%3 == 0 {
				isFull = true
			}
			err = buf.write(msg, isFull)
			if err != nil && errors.Is(err, ErrBufferFull) {
				//t.Log("buffer full, sync run!")
				buf.asyncRead()
			}
			time.Sleep(time.Millisecond)
		}

		ticker.Stop()
		close(closeCh)
	}()

	go func() {
		defer wg.Done()

		for {
			select {
			case <-closeCh:
				return
			case <-ticker.C:
				//t.Log("async ticker run!")
				buf.asyncRead()
			}
		}
	}()

	wg.Wait()
	t.Log("Done!")
	buf.Close()
}

func BenchmarkBuffer_Read_Write(b *testing.B) {
	walFile, err := os.OpenFile("./logs/wal.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	assert.Nil(b, err)
	buf := newBuffer(1024, walFile, DefaultBatchReadSize)
	defer buf.reset()

	closeCh := make(chan struct{})
	ticker := time.NewTicker(time.Second)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()

		for i := 0; i < b.N; i++ {
			msg := []byte(fmt.Sprintf("this is a test message, number: %d\n", i))
			isFull := false
			if i%3 == 0 {
				isFull = true
			}
			err = buf.write(msg, isFull)
			if err != nil && errors.Is(err, ErrBufferFull) {
				buf.asyncRead()
			}
			time.Sleep(time.Millisecond)
		}

		ticker.Stop()
		close(closeCh)
	}()

	go func() {
		defer wg.Done()

		for {
			select {
			case <-closeCh:
				return
			case <-ticker.C:
				//t.Log("async ticker run!")
				buf.asyncRead()
			}
		}
	}()

	wg.Wait()
	b.Log("Done!")
	buf.Close()
}
