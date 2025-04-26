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

//
//func TestNewWAL(t *testing.T) {
//	w, err := NewWAL(MixBufferSize, "./logs", DefaultBatchReadSize, time.Second)
//	assert.NoError(t, err)
//	defer w.Close()
//
//	template := "this is a test message, test number: %d\n"
//	for i := 0; i < 1000; i++ {
//		err = w.Write([]byte(fmt.Sprintf(template, i)), true)
//		if err != nil {
//			t.Logf("failed to write, cause: %s", err.Error())
//		}
//	}
//}
