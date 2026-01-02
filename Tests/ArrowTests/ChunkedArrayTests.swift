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

struct ChunkedArrayTests {

  @Test func chunkedRandomAccess() throws {
    let totalLength = 10000
    let numChunks = 20
    // Create reference flat array
    var flatArray: [Int32?] = []
    for i in 0..<totalLength {
      // Mix nulls and values
      flatArray.append(i % 7 == 0 ? nil : Int32(i))
    }
    // Split into random-sized chunks
    var chunks: [any ArrowArrayProtocol<Int32>] = []
    var offset = 0
    var remaining = totalLength
    for _ in 0..<numChunks {
      let chunkSize =
        remaining == 0
        ? 0
        : (remaining <= numChunks - chunks.count)
          ? 1 : Int.random(in: 1...(remaining - (numChunks - chunks.count - 1)))

      let builder = ArrayBuilderFixedWidth<Int32>()
      for i in 0..<chunkSize {
        let val = flatArray[offset + i]
        if let val {
          builder.append(val)
        } else {
          builder.appendNull()
        }
      }
      chunks.append(builder.finish())
      offset += chunkSize
      remaining -= chunkSize
    }
    let chunkedArray = try ChunkedArray(chunks.filter { $0.length > 0 })
    // Fuzz random accesses
    for _ in 0..<5000 {
      // TODO: consider an API which doesn't crash on invalid indexes
      //      let index = Int.random(in: -10..<totalLength + 10)
      let index = Int.random(in: 0..<totalLength)
      let chunkedValue = chunkedArray[index]
      let referenceValue = flatArray[index]
      #expect(
        chunkedValue == referenceValue,
        "Mismatch at index \(index): got \(chunkedValue as Any), expected \(referenceValue as Any)"
      )
    }
  }

  @Test func chunkBoundary() throws {
    let chunkSizes = [1, 5, 10, 100, 500, 1000]
    for size in chunkSizes {
      var flatArray: [Int32?] = []
      var chunks: [any ArrowArrayProtocol<Int32>] = []
      for chunkIdx in 0..<10 {
        let builder = ArrayBuilderFixedWidth<Int32>()
        for i in 0..<size {
          let value = Int32(chunkIdx * size + i)
          flatArray.append(value)
          builder.append(value)
        }
        chunks.append(builder.finish())
      }
      let chunkedArray = try ChunkedArray(chunks)
      // Test at every chunk boundary
      for chunkIdx in 0..<chunks.count {
        let boundaryIdx = chunkIdx * size
        // Before boundary
        if boundaryIdx > 0 {
          #expect(chunkedArray[boundaryIdx - 1] == flatArray[boundaryIdx - 1])
        }
        // At boundary
        #expect(chunkedArray[boundaryIdx] == flatArray[boundaryIdx])
        // After boundary
        if boundaryIdx + 1 < flatArray.count {
          #expect(chunkedArray[boundaryIdx + 1] == flatArray[boundaryIdx + 1])
        }
      }
    }
  }
}
