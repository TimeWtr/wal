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
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
)

func TestNewBuffer(t *testing.T) {
	walFile, err := os.OpenFile("./logs/wal.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	assert.Nil(t, err)
	buf := newBuffer(100000, walFile)
	defer buf.reset()

	closeCh := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-closeCh:
				return
			default:
				if buf.isFull {
					buf.asyncRead()
				}
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < 5000; i++ {
			msg := []byte(fmt.Sprintf("this is a test message, number: %d\n", i))
			err = buf.write(msg, true)
			if err != nil {
				assert.Equal(t, ErrBufferFull, err)
			}
		}

		close(closeCh)
	}()

	wg.Wait()
	t.Log("Done!")
}
