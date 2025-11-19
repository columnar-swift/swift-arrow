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

import Arrow
import Foundation
import Testing

struct FuzzedArrayTests {

  @Test func stringArrayWithRandomNulls() throws {
    var rng = getSeededRNG()
    let count = Int.random(in: 0...100_000)
    var testArray = [String?](repeating: nil, count: count)
    // Random strings with random nulls
    var utf8Count: Int = 0
    var nullCount: Int = 0
    for i in 0..<count {
      if Bool.random(using: &rng) {
        let length = Int.random(in: 0...100, using: &rng)
        let string = randomString(length: length, using: &rng)
        testArray[i] = string
        utf8Count += string.utf8.count
      } else {
        nullCount += 1
        testArray[i] = nil
      }
    }
    let builder: ArrayBuilderVariableLength<String> = .init()
    for value in testArray {
      if let value {
        builder.append(value)
      } else {
        builder.appendNull()
      }
    }
    let array = builder.finish()
    #expect(array.length == count)
    for i in 0..<count {
      #expect(array[i] == testArray[i])
    }
    #expect(array.nullCount == nullCount)
    let expectedNullBufferSize =
      switch nullCount {
      case 0, array.length: 0
      default: (count + 7) / 8
      }
    let expectedBufferSizes = [
      expectedNullBufferSize,
      4 * (count + 1),
      utf8Count,
    ]
    #expect(array.bufferSizes == expectedBufferSizes)
  }

  @Test func binaryStringArray() throws {
    let builder: ArrayBuilderVariableLength<Data> = .init()
    var byteCount: Int = 0
    let count: Int = 100
    var nullCount: Int = 0
    for index in 0..<count {
      if index % 10 == 9 {
        builder.appendNull()
        nullCount += 1
      } else {
        let val = Data("test\(index)".utf8)
        byteCount += val.count
        builder.append(val)
      }
    }
    let array = builder.finish()
    #expect(array.length == count)
    #expect(array.nullCount == nullCount)
    for index in 0..<array.length {
      if index % 10 == 9 {
        #expect(array[index] == nil)
      } else {
        let data = array[index]!
        let string = String(data: data, encoding: .utf8)
        #expect(string == "test\(index)")
      }
    }
    let expectedNullBufferSize =
      switch nullCount {
      case 0, array.length: 0
      default: (count + 7) / 8
      }
    let expectedBufferSizes: [Int] = [
      expectedNullBufferSize,
      4 * (count + 1),
      byteCount,
    ]
    #expect(array.bufferSizes == expectedBufferSizes)
  }

  @Test func binaryArrayWithRandomNulls() throws {
    var rng = getSeededRNG()
    let count = Int.random(in: 0...10_000)
    var byteCount: Int = 0
    var nullCount: Int = 0
    var expected = [Data?](repeating: nil, count: count)
    for i in 0..<count {
      if Bool.random(using: &rng) {
        let length = Int.random(in: 0...200, using: &rng)
        var data = Data(count: length)
        for j in 0..<length {
          data[j] = UInt8.random(in: 0...255, using: &rng)
        }
        expected[i] = data
        byteCount += length
      } else {
        expected[i] = nil
        nullCount += 1
      }
    }
    let builder: ArrayBuilderVariableLength<Data> = .init()
    for value in expected {
      if let value {
        builder.append(value)
      } else {
        builder.appendNull()
      }
    }
    let array = builder.finish()
    #expect(array.length == count)
    #expect(array.nullCount == nullCount)
    for i in 0..<count {
      #expect(array[i] == expected[i])
    }
    let expectedNullBufferSize =
      switch nullCount {
      case 0, array.length: 0
      default: (count + 7) / 8
      }
    let expectedBufferSizes: [Int] = [
      expectedNullBufferSize,
      4 * (count + 1),
      byteCount,
    ]
    #expect(array.bufferSizes == expectedBufferSizes)
  }

  @Test func int64ArrayWithRandomNulls() throws {
    var rng = getSeededRNG()
    let count = Int.random(in: 0...100_000)
    var nullCount: Int = 0
    var expected = [Int64?](repeating: nil, count: count)
    for i in 0..<count {
      if Bool.random(using: &rng) {
        expected[i] = Int64.random(in: Int64.min...Int64.max, using: &rng)
      } else {
        expected[i] = nil
        nullCount += 1
      }
    }
    let builder: ArrayBuilderFixedWidth<Int64> = .init()
    for value in expected {
      if let value {
        builder.append(value)
      } else {
        builder.appendNull()
      }
    }
    let array = builder.finish()
    #expect(array.length == count)
    #expect(array.nullCount == nullCount)
    for i in 0..<count {
      #expect(array[i] == expected[i])
    }
    let expectedNullBufferSize =
      switch nullCount {
      case 0, array.length: 0
      default: (count + 7) / 8
      }
    let expectedBufferSizes: [Int] = [
      expectedNullBufferSize,
      count * MemoryLayout<Int64>.stride,
    ]
    #expect(array.bufferSizes == expectedBufferSizes)
  }

  @Test func stringArrayVaryingNullDensity() throws {
    var rng = getSeededRNG()
    let densities = [0.0, 0.1, 0.5, 0.9, 1.0]
    for nullProbability in densities {
      let count = Int.random(in: 0...10_000)
      var byteCount: Int = 0
      var nullCount: Int = 0
      var expected = [String?](repeating: nil, count: count)
      for i in 0..<count {
        if Double.random(in: 0...1, using: &rng) > nullProbability {
          let length = Int.random(in: 0...50, using: &rng)
          expected[i] = randomString(length: length, using: &rng)
        }
      }
      let arrayBuilder: ArrayBuilderVariableLength<String> = .init()
      for value in expected {
        if let value {
          arrayBuilder.append(value)
          byteCount += value.utf8.count
        } else {
          arrayBuilder.appendNull()
          nullCount += 1
        }
      }
      let array = arrayBuilder.finish()
      #expect(array.length == count)
      #expect(array.nullCount == nullCount)
      for i in 0..<count {
        #expect(array[i] == expected[i])
      }
      let expectedNullBufferSize =
        switch nullCount {
        case 0, array.length: 0
        default: (count + 7) / 8
        }
      let expectedBufferSizes = [
        expectedNullBufferSize,
        (count + 1) * 4,
        byteCount,
      ]
      #expect(array.bufferSizes == expectedBufferSizes)
    }
  }

  @Test
  func stringArrayEdgeCases() throws {
    var rng = getSeededRNG()
    let count = 1000
    var byteCount: Int = 0
    var nullCount: Int = 0
    var expected = [String?](repeating: nil, count: count)
    for i in 0..<count {
      switch Int.random(in: 0...6, using: &rng) {
      case 0:
        expected[i] = ""  // Empty string
      case 1:
        expected[i] = randomString(length: 1, using: &rng)
      case 2:
        expected[i] = randomString(length: 10000, using: &rng)
      case 3:
        expected[i] = String(repeating: "a", count: 100)
      case 4:
        expected[i] = "🎉🚀✨"
      case 5:
        expected[i] = nil
      default:
        expected[i] = randomString(length: Int.random(in: 1..<100), using: &rng)
      }
      if let value = expected[i] {
        byteCount += value.utf8.count
      }
    }
    let builder: ArrayBuilderVariableLength<String> = .init()
    for value in expected {
      if let value {
        builder.append(value)
      } else {
        builder.appendNull()
        nullCount += 1
      }
    }
    let array = builder.finish()
    #expect(array.length == count)
    #expect(array.nullCount == nullCount)
    for i in 0..<count {
      #expect(array[i] == expected[i])
    }
    let expectedNullBufferSize =
      switch nullCount {
      case 0, array.length: 0
      default: (count + 7) / 8
      }
    let expectedBufferSizes = [
      expectedNullBufferSize,
      (count + 1) * 4,
      byteCount,
    ]
    #expect(array.bufferSizes == expectedBufferSizes)
  }

  @Test func consecutiveNulls() throws {
    var rng = getSeededRNG()
    let count: Int = 10_000
    var nullCount: Int = 0
    var expected = [Int64?](repeating: nil, count: count)
    var i = 0
    while i < count {
      let runLength = Int.random(in: 1...100, using: &rng)
      let isNull = Bool.random(using: &rng)
      for j in 0..<min(runLength, count - i) {
        if !isNull {
          expected[i + j] = Int64.random(in: Int64.min...Int64.max, using: &rng)
        }
      }
      i += runLength
    }
    let builder: ArrayBuilderFixedWidth<Int64> = .init()
    for value in expected {
      if let value {
        builder.append(value)
      } else {
        builder.appendNull()
        nullCount += 1
      }
    }
    let array = builder.finish()
    #expect(array.length == count)
    #expect(array.nullCount == nullCount)
    for i in 0..<count {
      #expect(array[i] == expected[i])
    }
    let expectedNullBufferSize =
      switch nullCount {
      case 0, array.length: 0
      default: (count + 7) / 8
      }
    let expectedBufferSizes = [
      expectedNullBufferSize,
      count * MemoryLayout<Int64>.stride,
    ]
    #expect(array.bufferSizes == expectedBufferSizes)
  }
}
