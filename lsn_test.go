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
	"testing"
)

func TestLSN_Encode_Decode(t *testing.T) {
	l := LSN{
		Timeline:  0x3F,          // 最大6位值
		MachineID: 0x3FF,         // 最大10位值
		Timestamp: 0x1FFFFFFFFFF, // 最大41位值
		couter:    0x7F,          // 最大7位值
	}

	encoded := encode(l)
	t.Logf("lsn: %d\n", encoded)
	decoded := decode(encoded)

	t.Logf("%+v", decoded)
}

func TestLSN_Encode(t *testing.T) {
	l := LSN{
		Timeline:  00000002,
		MachineID: 1022,
		Timestamp: 647330365878,
		couter:    110,
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
			MachineID: 1022,
			Timestamp: 647330365878,
			couter:    110,
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
			MachineID: 1022,
			Timestamp: 647330365878,
			couter:    110,
		}

		encoded := encode(l)
		decode(encoded)
	}
}

func TestLsnGenerator_Next(t *testing.T) {
	gen := newLsnGenerator(10, 00000001)
	for i := 0; i < 100; i++ {
		lsn, err := gen.Next()
		assert.NoError(t, err)
		t.Logf("lsn: %d\n", lsn)
	}
}

func TestLsnGenerator_Next_Counter_Too_Long(t *testing.T) {
	gen := newLsnGenerator(10, 00000001)
	for i := 0; i < 1000; i++ {
		lsn, err := gen.Next()
		if err != nil {
			assert.Equal(t, err, fmt.Errorf("lsn generator counter too big"))
			t.Log("counter too big")
			continue
		}
		t.Logf("lsn: %d\n", lsn)
	}
}

func TestLsnGenerator_Next_Counter_CurrentLSN(t *testing.T) {
	gen := newLsnGenerator(10, 00000001)
	for i := 0; i < 1000; i++ {
		_, err := gen.Next()
		if err != nil {
			assert.Equal(t, err, fmt.Errorf("lsn generator counter too big"))
			t.Log("counter too big")
			continue
		}
		
		t.Logf("current lsn encode: %d\n", encode(gen.Current()))
	}
}
