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

struct ArrayTests2 {

  @Test func boolArray() throws {
    let builder = ArrayBuilderBoolean()
    builder.append(true)
    builder.appendNull()
    builder.append(false)
    builder.append(false)
    let array = builder.finish()
    #expect(array.length == 4)
    #expect(array[0]! == true)
    #expect(array[1] == nil)
    #expect(array[2]! == false)
    #expect(array[3]! == false)
    #expect(array.bufferSizes == [1, 1])
  }

  @Test func uint8Array() throws {

    // MARK: UInt8 array
    let arrayBuilder: ArrayBuilderFixedWidth<UInt8> = .init()
    for index: UInt8 in 0..<100 {
      arrayBuilder.append(index)
    }
    arrayBuilder.appendNull()
    #expect(arrayBuilder.length == 101)
    let array = arrayBuilder.finish()
    #expect(array.length == 101)
    #expect(array[1] == 1)
    #expect(array[10] == 10)
    #expect(array[100] == nil)
    for index in 0..<100 {
      #expect(array[Int(index)] == UInt8(index))
    }
    let slice = array.slice(offset: 5, length: 5)
    for i in 0..<5 {
      #expect(slice[i] == UInt8(5 + i))
    }
    #expect(array.bufferSizes == [(101 + 7) / 8, 101])
  }

  @Test func int64Array() throws {
    var rng = getSeededRNG()
    let count = Int.random(in: 0...100_000)
    var expected = [Int64](repeating: 0, count: count)
    for i in 0..<expected.count {
      expected[i] = Int64.random(in: Int64.min...Int64.max, using: &rng)
    }
    let arrayBuilder: ArrayBuilderFixedWidth<Int64> = .init()
    for i in 0..<expected.count {
      arrayBuilder.append(expected[i])
    }
    let array = arrayBuilder.finish()

    for i in 0..<expected.count {
      #expect(array[i] == expected[i])
    }
    #expect(array.bufferSizes == [0, count * MemoryLayout<Int>.stride])
  }

  @Test func stringArray() throws {
    let builder: ArrayBuilderVariableLength<String> = .init()

    builder.appendNull()
    builder.append("abc")
    builder.append("def")
    builder.appendNull()
    builder.append("This is a longer string")
    builder.appendNull()
    builder.appendNull()
    for i in 0..<100 {
      builder.append("test \(i)")
    }

    let array = builder.finish()
    #expect(array[0] == nil)
    #expect(array[1] == "abc")
    #expect(array[2] == "def")
    #expect(array[3] == nil)
    #expect(array[4] == "This is a longer string")
    #expect(array[5] == nil)
    #expect(array[6] == nil)
    var utf8Count = 3 + 3 + 23
    for i in 0..<100 {
      utf8Count += "test \(i)".utf8.count
      #expect(array[i + 7] == "test \(i)")
    }
    #expect(array.length == 107)
    #expect(array.bufferSizes == [(107 + 7) / 8, (107 + 1) * 4, utf8Count])
  }

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

    let arrayBuilder: ArrayBuilderVariableLength<String> = .init()
    for value in testArray {
      if let value {
        arrayBuilder.append(value)
      } else {
        arrayBuilder.appendNull()
      }
    }
    let array = arrayBuilder.finish()
    #expect(array.length == count)
    for i in 0..<count {
      #expect(array[i] == testArray[i])
    }
    let expectedBufferSizes = [
      (count + 7) / 8,
      4 * (count + 1),
      utf8Count,
    ]
    #expect(array.bufferSizes == expectedBufferSizes)
  }

  @Test func binaryStringArray() throws {
    let arrayBuilder: ArrayBuilderVariableLength<Data> = .init()
    var byteCount: Int = 0
    for index in 0..<100 {
      if index % 10 == 9 {
        arrayBuilder.appendNull()
      } else {
        let val = Data("test\(index)".utf8)
        byteCount += val.count
        arrayBuilder.append(val)
      }
    }
    let array = arrayBuilder.finish()
    #expect(array.length == 100)
    for index in 0..<array.length {
      if index % 10 == 9 {
        #expect(array[index] == nil)
      } else {
        let data = array[index]!
        let string = String(data: data, encoding: .utf8)
        #expect(string == "test\(index)")
      }
    }
    #expect(array.bufferSizes == [(100 + 7) / 8, 4 * (100 + 1), byteCount])
  }

  @Test func binaryArrayWithRandomNulls() throws {
    var rng = getSeededRNG()
    let count = Int.random(in: 0...10_000)
    var byteCount: Int = 0
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
      }
    }
    let arrayBuilder: ArrayBuilderVariableLength<Data> = .init()
    for value in expected {
      if let value {
        arrayBuilder.append(value)
      } else {
        arrayBuilder.appendNull()
      }
    }
    let array = arrayBuilder.finish()
    for i in 0..<count {
      #expect(array[i] == expected[i])
    }
    #expect(
      array.bufferSizes == [(count + 7) / 8, 4 * (count + 1), byteCount])
  }

  @Test func int64ArrayWithRandomNulls() throws {
    var rng = getSeededRNG()
    let count = Int.random(in: 0...100_000)
    var expected = [Int64?](repeating: nil, count: count)
    for i in 0..<count {
      if Bool.random(using: &rng) {
        expected[i] = Int64.random(in: Int64.min...Int64.max, using: &rng)
      } else {
        expected[i] = nil
      }
    }
    let arrayBuilder: ArrayBuilderFixedWidth<Int64> = .init()
    for value in expected {
      if let value {
        arrayBuilder.append(value)
      } else {
        arrayBuilder.appendNull()
      }
    }
    let array = arrayBuilder.finish()
    for i in 0..<count {
      #expect(array[i] == expected[i])
    }
    let expectedBufferSizes = [
      (count + 7) / 8,
      count * MemoryLayout<Int64>.stride,
    ]
    #expect(array.bufferSizes == expectedBufferSizes)
  }

  @Test func stringArrayVaryingNullDensity() throws {
    var rng = getSeededRNG()

    // Test different null densities
    let densities = [0.0, 0.1, 0.5, 0.9, 1.0]

    for nullProbability in densities {
      let count = Int.random(in: 0...10_000)
      var byteCount: Int = 0
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
        }
      }
      let array = arrayBuilder.finish()
      for i in 0..<count {
        #expect(array[i] == expected[i])
      }
      let expectedNullBufferSize =
        switch array.nullCount {
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
    var byteCount = 0
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
    let arrayBuilder: ArrayBuilderVariableLength<String> = .init()
    for value in expected {
      if let value {
        arrayBuilder.append(value)
      } else {
        arrayBuilder.appendNull()
      }
    }
    let stringArray = arrayBuilder.finish()
    for i in 0..<count {
      #expect(stringArray[i] == expected[i])
    }
    // TODO: buffer count checks
  }

  @Test func consecutiveNulls() throws {
    var rng = getSeededRNG()
    let count = 10_000
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
    let arrayBuilder: ArrayBuilderFixedWidth<Int64> = .init()
    for value in expected {
      if let value {
        arrayBuilder.append(value)
      } else {
        arrayBuilder.appendNull()
      }
    }
    let int64Array = arrayBuilder.finish()
    for i in 0..<count {
      #expect(int64Array[i] == expected[i])
    }
  }

  @Test func doubleArray() throws {
    let builder: ArrayBuilderFixedWidth<Double> = .init()
    builder.append(14)
    builder.appendNull()
    builder.append(40.4)
    let doubleArray = builder.finish()
    #expect(doubleArray.length == 3)
    #expect(doubleArray[0]! == 14)
    #expect(doubleArray[1] == nil)
    #expect(doubleArray[2]! == 40.4)
  }

  @Test func date32Array() throws {
    let date32Builder: ArrayBuilderDate32 = .init()
    let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
    let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
    date32Builder.append(date1)
    date32Builder.append(date2)
    date32Builder.appendNull()
    let date32Array = date32Builder.finish()
    #expect(date32Array.length == 3)
    #expect(date32Array[1] == date2)
    let adjustedDate1 = Date(
      timeIntervalSince1970: date1.timeIntervalSince1970 - 352)
    #expect(date32Array[0]! == adjustedDate1)
  }

  @Test func date64Array() throws {
    let date64Builder: ArrayBuilderDate64 = .init()
    let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
    let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
    date64Builder.append(date1)
    date64Builder.append(date2)
    date64Builder.appendNull()
    let date64Array = date64Builder.finish()
    #expect(date64Array.length == 3)
    #expect(date64Array[1] == date2)
    #expect(date64Array[0]! == date1)
  }

  @Test func time32Array() throws {
    let milliBuilder: ArrayBuilderTime32 = .init()
    milliBuilder.append(100)
    milliBuilder.append(1_000_000)
    milliBuilder.appendNull()
    let milliArray = milliBuilder.finish()
    #expect(milliArray.length == 3)
    #expect(milliArray[1] == 1_000_000)
    #expect(milliArray[2] == nil)

    let secBuilder: ArrayBuilderTime32 = .init()
    secBuilder.append(200)
    secBuilder.appendNull()
    secBuilder.append(2_000_011)
    let secArray = secBuilder.finish()
    #expect(secArray.length == 3)
    #expect(secArray[1] == nil)
    #expect(secArray[2] == 2_000_011)
  }

  @Test func time64Array() throws {
    let nanoBuilder: ArrayBuilderTime64 = .init()
    nanoBuilder.append(10000)
    nanoBuilder.appendNull()
    nanoBuilder.append(123_456_789)
    let nanoArray = nanoBuilder.finish()
    #expect(nanoArray.length == 3)
    #expect(nanoArray[1] == nil)
    #expect(nanoArray[2] == 123_456_789)

    let microBuilder: ArrayBuilderTime64 = .init()
    microBuilder.appendNull()
    microBuilder.append(20000)
    microBuilder.append(987_654_321)

    let microArray = microBuilder.finish()
    #expect(microArray.length == 3)
    #expect(microArray[1] == 20000)
    #expect(microArray[2] == 987_654_321)
  }

  @Test func timestampArray() throws {
    // Test timestamp with seconds unit
    let secBuilder: ArrayBuilderTimestamp = .init()
    secBuilder.append(1_609_459_200)  // 2021-01-01 00:00:00
    secBuilder.append(1_609_545_600)  // 2021-01-02 00:00:00
    secBuilder.appendNull()
    let secArray = secBuilder.finish()
    #expect(secArray.length == 3)
    #expect(secArray[0] == 1_609_459_200)
    #expect(secArray[1] == 1_609_545_600)
    #expect(secArray[2] == nil)

    // Test timestamp with milliseconds unit and timezone America/New_York
    let msBuilder: ArrayBuilderTimestamp = .init()
    msBuilder.append(1_609_459_200_000)  // 2021-01-01 00:00:00.000
    msBuilder.appendNull()
    msBuilder.append(1_609_545_600_000)  // 2021-01-02 00:00:00.000
    let msArray = msBuilder.finish()
    #expect(msArray.length == 3)
    #expect(msArray[0] == 1_609_459_200_000)
    #expect(msArray[1] == nil)
    #expect(msArray[2] == 1_609_545_600_000)

    // Test timestamp with microseconds unit and timezone UTC
    let usBuilder: ArrayBuilderTimestamp = .init()
    usBuilder.append(1_609_459_200_000_000)  // 2021-01-01 00:00:00.000000
    usBuilder.append(1_609_545_600_000_000)  // 2021-01-02 00:00:00.000000
    usBuilder.append(1_609_632_000_000_000)  // 2021-01-03 00:00:00.000000
    let usArray = usBuilder.finish()
    #expect(usArray.length == 3)
    #expect(usArray[0] == 1_609_459_200_000_000)
    #expect(usArray[1] == 1_609_545_600_000_000)
    #expect(usArray[2] == 1_609_632_000_000_000)

    // Test timestamp with nanoseconds unit
    let nsBuilder: ArrayBuilderTimestamp = .init()
    nsBuilder.appendNull()
    // 2021-01-01 00:00:00.000000000
    nsBuilder.append(1_609_459_200_000_000_000)
    // 2021-01-02 00:00:00.000000000
    nsBuilder.append(1_609_545_600_000_000_000)
    let nsArray = nsBuilder.finish()
    #expect(nsArray.length == 3)
    #expect(nsArray[0] == nil)
    #expect(nsArray[1] == 1_609_459_200_000_000_000)
    #expect(nsArray[2] == 1_609_545_600_000_000_000)
  }
}
