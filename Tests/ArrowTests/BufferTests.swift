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

import Foundation
import Testing

@testable import Arrow

struct BufferTests {

  @Test func nullBufferBuilder() {

    // TODO: consider empty buffers

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
    #expect(nullBuffer.capacity - nullBuffer.length < 64)

    let dataAddress = UInt(bitPattern: nullBuffer.buffer)
    #expect(dataAddress % 64 == 0, "Buffer should be 64-byte aligned")
  }

  @Test func fixedWidthBufferBuilder() {

    let builder = FixedWidthBufferBuilder<Int64>()

    for i in 0..<10_000 {
      builder.append(Int64(i))
    }

    let buffer = builder.finish()
    #expect(buffer.length == 10000)

  }

  @Test func offsetsBuffer() {

    let offsets: [UInt32] = [0, 4, 8, 12, 16, 20]
    let data = offsets.withUnsafeBufferPointer { buffer in
      Data(
        buffer: UnsafeBufferPointer(
          start: buffer.baseAddress,
          count: buffer.count)
      )
    }
    #expect(data.count == 24)
    let range = 0..<data.count
    let borrowed = BorrowedOffsets(
      count: offsets.count - 1, data: data, range: range)

    var (start, end) = borrowed.offsets(at: 2)
    #expect(start == 8)
    #expect(end == 12)

    (start, end) = borrowed.offsets(at: 4)
    #expect(start == 16)
    #expect(end == 20)
  }
}
