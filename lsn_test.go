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
	"sync"
	"testing"
	"time"
)

func TestLSN_Encode_Decode(t *testing.T) {
	l := LSN{
		Timeline:  0x3F,          // 最大6位值
		CenterID:  0x1F,          // 最大5位值
		MachineID: 0x1F,          // 最大5位值
		Timestamp: 0x1FFFFFFFFFF, // 最大41位值
		counter:   0x7F,          // 最大7位值
	}

	encoded := encode(l)
	t.Logf("lsn: %d\n", encoded)
	decoded := decode(encoded)

	t.Logf("%+v", decoded)
}

func TestLSN_Encode(t *testing.T) {
	l := LSN{
		Timeline:  00000002,
		CenterID:  23,
		MachineID: 10,
		Timestamp: 647330365878,
		counter:   110,
	}

	encoded := encode(l)
	t.Logf("lsn: %d\n", encoded)
	decoded := decode(encoded)

	t.Logf("%+v", decoded)
}

func TestLSN_Decode(t *testing.T) {
	encoded := uint64(864351774276901742)
	decoded := decode(encoded)

	t.Logf("%+v", decoded)
}

func BenchmarkLSN_Encode_Decode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		l := LSN{
			Timeline:  00000002,
			CenterID:  12,
			MachineID: 12,
			Timestamp: 647330365878,
			counter:   110,
		}

		encoded := encode(l)
		b.Logf("lsn: %d\n", encoded)
		decoded := decode(encoded)

		b.Logf("%+v", decoded)
	}
}

func BenchmarkLSN_Encode_Decode_NoLog(b *testing.B) {
	for i := 0; i < b.N; i++ {
		l := LSN{
			Timeline:  00000002,
			CenterID:  122,
			MachineID: 102,
			Timestamp: 647330365878,
			counter:   110,
		}

		encoded := encode(l)
		decode(encoded)
	}
}

func TestLsnGenerator_Next(t *testing.T) {
	gen := newLsnGenerator(10, 10, 00000001, time.Now())
	defer gen.Close()

	for i := 0; i < 127; i++ {
		lsn, err := gen.Next()
		assert.NoError(t, err)
		t.Logf("lsn: %d\n", lsn)
	}
}

func TestLsnGenerator_Next_Counter_Overflow(t *testing.T) {
	gen := newLsnGenerator(10, 10, 00000001, time.Now())
	defer gen.Close()

	for i := 0; i < 1000; i++ {
		lsn, err := gen.Next()
		if err != nil {
			t.Logf("failed to generate lsn, timestamp: %d, cause: %s\n", time.Now().UnixMilli(), err.Error())
		} else {
			assert.NoError(t, err)
			t.Logf("lsn: %d\n", lsn)
		}
	}
}

func TestLsnGenerator_Next_Counter_Single(t *testing.T) {
	gen := newLsnGenerator(10, 10, 00000001, time.Now())
	defer gen.Close()

	counter := 0
	now := time.Now()
	for {
		if time.Now().Sub(now) > time.Second {
			break
		}

		lsn, err := gen.Next()
		if err != nil {
			t.Logf("failed to generate lsn, timestamp: %d, cause: %s\n", time.Now().UnixMilli(), err.Error())
		} else {
			counter++
			assert.NoError(t, err)
			t.Logf("lsn: %d\n", lsn)
		}
	}

	t.Logf("counter: %d\n", counter)
}

func TestLsnGenerator_Next_Concurrent(t *testing.T) {
	gen := newLsnGenerator(12, 10, 00000001, time.Now())
	defer gen.Close()

	var wg sync.WaitGroup
	for i := 0; i < 127; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			lsn, err := gen.Next()
			assert.NoError(t, err)
			t.Logf("lsn: %d\n", lsn)
		}()
	}
	wg.Wait()
}

func TestLsnGenerator_ResetEpoch(t *testing.T) {
	gen := newLsnGenerator(10, 10, 00000001, time.Now())
	defer gen.Close()

	counter := 0
	failCounter := 0
	now := time.Now()
	for {
		if time.Now().Sub(now) > time.Second {
			break
		}

		if time.Now().Sub(now) >= time.Millisecond*20 && time.Now().Sub(now) < time.Millisecond*30 {
			gen.ResetEpoch(time.Now().Add(3 * time.Millisecond))
			t.Log("reset epoch!")
		}

		lsn, err := gen.Next()
		if err != nil {
			failCounter++
			t.Logf("failed to generate lsn, timestamp: %d, cause: %s\n", time.Now().UnixMilli(), err.Error())
		} else {
			counter++
			assert.NoError(t, err)
			t.Logf("lsn: %d\n", lsn)
		}
	}

	t.Logf("counter: %d\n", counter)
	t.Logf("fail counter: %d\n", failCounter)
}

func TestLsnGenerator_Next_Counter_Too_Long(t *testing.T) {
	gen := newLsnGenerator(12, 10, 00000001, time.Now())
	defer gen.Close()

	for i := 0; i < 1000; i++ {
		lsn, err := gen.Next()
		if err != nil {
			assert.Equal(t, err, fmt.Errorf("counter overflow"))
			t.Log("counter overflow")
			continue
		}
		t.Logf("lsn: %d\n", lsn)
	}
}

func TestLsnGenerator_Next_Counter_CurrentLSN(t *testing.T) {
	gen := newLsnGenerator(12, 10, 00000001, time.Now())
	defer gen.Close()

	for i := 0; i < 1000; i++ {
		_, err := gen.Next()
		if err != nil {
			assert.Equal(t, err, fmt.Errorf("counter overflow"))
			t.Log("counter overflow")
			continue
		}

		t.Logf("current lsn encode: %d\n", encode(gen.Current()))
	}
}

func TestLSNManager_Next(t *testing.T) {
	gen := newLsnGenerator(10, 10, 00000001, time.Now())
	sto, err := newFileStorage("./logs")
	assert.NoError(t, err)
	lsm, err := newLSNManager(gen, sto)
	assert.NoError(t, err)
	defer lsm.Close()

	for i := 0; i < 10000; i++ {
		lsn, er := lsm.Next()
		if er != nil {
			t.Logf("failed to generate lsn, timestamp: %d, cause: %s\n", time.Now().UnixMilli(), er.Error())
			continue
		}
		t.Logf("lsn: %d\n", lsn)
	}
}

func TestLSNManager_Next_Storage(t *testing.T) {
	gen := newLsnGenerator(10, 18, 00000001, time.Now())
	sto, err := newFileStorage("./logs")
	assert.NoError(t, err)
	lsm, err := newLSNManager(gen, sto)
	assert.NoError(t, err)
	defer lsm.Close()

	for i := 0; i < 100000; i++ {
		if i%100 == 0 {
			if err = lsm.Save(); err != nil {
				t.Logf("failed to save lsn, cause: %s\n", err.Error())
			}
		}
		if i%1000 == 0 {
			if err = lsm.Sync(); err != nil {
				t.Logf("failed to sync lsn, cause: %s\n", err.Error())
			}
		}
		lsn, er := lsm.Next()
		if er != nil {
			t.Logf("failed to generate lsn, timestamp: %d, cause: %s\n", time.Now().UnixMilli(), er.Error())
			continue
		}
		t.Logf("lsn: %d\n", lsn)
	}
}
