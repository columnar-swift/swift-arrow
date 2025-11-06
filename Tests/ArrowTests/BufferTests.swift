// Copyright 2025 The Apache Software Foundation
// Copyright 2025 The Columnar Swift Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Testing

@testable import Arrow

struct BufferTests {

  @Test func nullBufferBuilder() {
    let mutableNullBuffer = NullBufferBuilder()
    for i in 0..<10000 {
      if i % 7 == 0 {
        mutableNullBuffer.appendValid(true)
      } else {
        mutableNullBuffer.appendValid(false)
      }
    }
    let nullBuffer = mutableNullBuffer.finish()
    for i in 0..<10000 {
      if i % 7 == 0 {
        #expect(nullBuffer.isSet(i))
      } else {
        #expect(!nullBuffer.isSet(i))
      }
    }
    #expect(nullBuffer.capacity % 64 == 0)
    //
    #expect(nullBuffer.capacity - nullBuffer.length < 64)

    let dataAddress = UInt(bitPattern: nullBuffer.buffer)
    #expect(dataAddress % 64 == 0, "Buffer should be 64-byte aligned")
  }
}
