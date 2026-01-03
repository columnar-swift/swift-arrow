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

/// Test internal null buffer types are correct.
struct ArrayNullBufferTests {

  @Test func allValidValues() throws {
    // Should be able to omit null buffer entirely
    let arrayBuilder: ArrayBuilderFixedWidth<Int64> = .init()
    for i in 0..<1000 {
      arrayBuilder.append(Int64(i))  // No nulls
    }
    let array = arrayBuilder.finish()
    for i in 0..<1000 {
      #expect(array[i]! == Int64(i))
    }
    let nullBuffer = try #require(array.nullBuffer as? AllValidNullBuffer)
    #expect(nullBuffer.valueCount == 1000)
    #expect(array.bufferSizes == [0, 1000 * MemoryLayout<Int64>.stride])
  }

  @Test func allNullValues() throws {
    let arrayBuilder: ArrayBuilderFixedWidth<Int64> = .init()
    for _ in 0..<1000 {
      arrayBuilder.appendNull()
    }
    let array = arrayBuilder.finish()
    for i in 0..<1000 {
      #expect(array[i] == nil)
    }
    let nullBuffer = try #require(array.nullBuffer as? AllNullBuffer)
    #expect(nullBuffer.valueCount == 1000)
    #expect(array.bufferSizes == [0, 1000 * MemoryLayout<Int64>.stride])
  }
}
