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

struct ArrayTests {

  @Test func uint8Array() throws {

    // MARK: UInt8 array
    let arrayBuilder: NumberArrayBuilder<UInt8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    for index in 0..<100 {
      arrayBuilder.append(UInt8(index))
    }

    #expect(arrayBuilder.nullCount == 0)
    arrayBuilder.append(nil)
    #expect(arrayBuilder.length == 101)
    #expect(arrayBuilder.capacity == 128)
    #expect(arrayBuilder.nullCount == 1)
    let array = try arrayBuilder.finish()
    #expect(array.length == 101)
    #expect(array[1]! == 1)
    #expect(array[10]! == 10)
    #expect(try array.isNull(at: 100) == true)

    for buffer in array.arrowData.buffers {
      let dataAddress = UInt(bitPattern: buffer.rawPointer)
      #expect(dataAddress % 64 == 0, "Buffer should be 64-byte aligned")
    }
  }

  @Test func doubleArray() throws {

    // MARK: Double array
    let doubleBuilder: NumberArrayBuilder<Double> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    doubleBuilder.append(14)
    doubleBuilder.append(40.4)
    #expect(doubleBuilder.nullCount == 0)
    #expect(doubleBuilder.length == 2)
    #expect(doubleBuilder.capacity == 256)
    let doubleArray = try doubleBuilder.finish()
    #expect(doubleArray.length == 2)
    #expect(doubleArray[0]! == 14)
    #expect(doubleArray[1]! == 40.4)
  }

  @Test func stringArray() throws {
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    for index in 0..<100 {
      if index % 10 == 9 {
        stringBuilder.append(nil)
      } else {
        stringBuilder.append("test" + String(index))
      }
    }

    #expect(stringBuilder.nullCount == 10)
    #expect(stringBuilder.length == 100)
    #expect(stringBuilder.capacity == 640)
    let stringArray = try stringBuilder.finish()
    #expect(stringArray.length == 100)
    for index in 0..<stringArray.length {
      if index % 10 == 9 {
        #expect(try stringArray.isNull(at: index) == true)
      } else {
        #expect(stringArray[index]! == "test" + String(index))
      }
    }

    #expect(stringArray[1]! == "test1")
    #expect(stringArray[0]! == "test0")
  }

  @Test func boolArray() throws {

    for i in 0..<100 {
      let bytesNeeded = (i + 7) / 8
      print("\(i) bits requires \(bytesNeeded) bytes")
    }

    let boolBuilder = try ArrowArrayBuilders.loadBoolArrayBuilder()
    boolBuilder.append(true)
    boolBuilder.append(nil)
    boolBuilder.append(false)
    boolBuilder.append(false)
    #expect(boolBuilder.nullCount == 1)
    #expect(boolBuilder.length == 4)
    #expect(boolBuilder.capacity == 64)
    let boolArray = try boolBuilder.finish()
    #expect(boolArray.length == 4)
    #expect(boolArray[0]! == true)
    #expect(boolArray[1] == nil)
    #expect(boolArray[2]! == false)
    #expect(boolArray[3]! == false)
  }

  //  Uncomment if you want other tests to crash with memory corruption issues.
  //  @Test func boolArrayNils() throws {
  //
  //    let boolBuilder = try ArrowArrayBuilders.loadBoolArrayBuilder()
  //    boolBuilder.append(true)
  //    for i in 0..<10000 {
  //      boolBuilder.append(nil)
  //    }
  //    let boolArray = try boolBuilder.finish()
  //    #expect(boolArray.nullCount == 10000)
  //  }

  @Test func date32Array() throws {
    let date32Builder: Date32ArrayBuilder =
      try ArrowArrayBuilders.loadDate32ArrayBuilder()
    let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
    let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
    date32Builder.append(date1)
    date32Builder.append(date2)
    date32Builder.append(nil)
    #expect(date32Builder.nullCount == 1)
    #expect(date32Builder.length == 3)
    #expect(date32Builder.capacity == 128)
    let date32Array = try date32Builder.finish()
    #expect(date32Array.length == 3)
    #expect(date32Array[1] == date2)
    let adjustedDate1 = Date(
      timeIntervalSince1970: date1.timeIntervalSince1970 - 352)
    #expect(date32Array[0]! == adjustedDate1)
  }

  @Test func date64Array() throws {
    let date64Builder: Date64ArrayBuilder =
      try ArrowArrayBuilders.loadDate64ArrayBuilder()
    let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
    let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
    date64Builder.append(date1)
    date64Builder.append(date2)
    date64Builder.append(nil)
    #expect(date64Builder.nullCount == 1)
    #expect(date64Builder.length == 3)
    #expect(date64Builder.capacity == 256)
    let date64Array = try date64Builder.finish()
    #expect(date64Array.length == 3)
    #expect(date64Array[1] == date2)
    #expect(date64Array[0]! == date1)
  }

  @Test func binaryArray() throws {
    let binaryBuilder = try ArrowArrayBuilders.loadBinaryArrayBuilder()
    for index in 0..<100 {
      if index % 10 == 9 {
        binaryBuilder.append(nil)
      } else {
        binaryBuilder.append(("test" + String(index)).data(using: .utf8))
      }
    }

    #expect(binaryBuilder.nullCount == 10)
    #expect(binaryBuilder.length == 100)
    #expect(binaryBuilder.capacity == 640)
    let binaryArray = try binaryBuilder.finish()
    #expect(binaryArray.length == 100)
    for index in 0..<binaryArray.length {
      if index % 10 == 9 {
        #expect(try binaryArray.isNull(at: index) == true)
      } else {
        let stringData = String(bytes: binaryArray[index]!, encoding: .utf8)
        #expect(stringData == "test" + String(index))
      }
    }
  }

  @Test func time32Array() throws {
    let milliBuilder = try ArrowArrayBuilders.loadTime32ArrayBuilder(
      .millisecond)
    milliBuilder.append(100)
    milliBuilder.append(1_000_000)
    milliBuilder.append(nil)
    #expect(milliBuilder.nullCount == 1)
    #expect(milliBuilder.length == 3)
    #expect(milliBuilder.capacity == 128)
    let milliArray = try milliBuilder.finish()
    guard case .time32(let milliType) = milliArray.arrowData.type else {
      Issue.record("Expected time32")
      return
    }
    #expect(milliType == .millisecond)
    #expect(milliArray.length == 3)
    #expect(milliArray[1] == 1_000_000)
    #expect(milliArray[2] == nil)

    let secBuilder = try ArrowArrayBuilders.loadTime32ArrayBuilder(.second)
    secBuilder.append(200)
    secBuilder.append(nil)
    secBuilder.append(2_000_011)
    #expect(secBuilder.nullCount == 1)
    #expect(secBuilder.length == 3)
    #expect(secBuilder.capacity == 128)
    let secArray = try secBuilder.finish()
    guard case .time32(let secType) = secArray.arrowData.type else {
      Issue.record("Expected time32")
      return
    }
    #expect(secType == .second)
    #expect(secArray.length == 3)
    #expect(secArray[1] == nil)
    #expect(secArray[2] == 2_000_011)
  }

  @Test func time64Array() throws {
    let nanoBuilder = try ArrowArrayBuilders.loadTime64ArrayBuilder(
      .nanosecond)
    nanoBuilder.append(10000)
    nanoBuilder.append(nil)
    nanoBuilder.append(123_456_789)
    #expect(nanoBuilder.nullCount == 1)
    #expect(nanoBuilder.length == 3)
    #expect(nanoBuilder.capacity == 256)
    let nanoArray = try nanoBuilder.finish()
    guard case .time64(let nanoType) = nanoArray.arrowData.type else {
      Issue.record("Expected time64")
      return
    }
    #expect(nanoType == .nanosecond)
    #expect(nanoArray.length == 3)
    #expect(nanoArray[1] == nil)
    #expect(nanoArray[2] == 123_456_789)

    let microBuilder = try ArrowArrayBuilders.loadTime64ArrayBuilder(
      .microsecond
    )
    microBuilder.append(nil)
    microBuilder.append(20000)
    microBuilder.append(987_654_321)
    #expect(microBuilder.nullCount == 1)
    #expect(microBuilder.length == 3)
    #expect(microBuilder.capacity == 256)
    let microArray = try microBuilder.finish()
    guard case .time64(let microType) = microArray.arrowData.type else {
      Issue.record("Expected time64")
      return
    }
    #expect(microType == .microsecond)
    #expect(microArray.length == 3)
    #expect(microArray[1] == 20000)
    #expect(microArray[2] == 987_654_321)
  }

  @Test func timestampArray() throws {
    // Test timestamp with seconds unit
    let secBuilder = try ArrowArrayBuilders.loadTimestampArrayBuilder(
      .second,
      timezone: nil
    )
    secBuilder.append(1_609_459_200)  // 2021-01-01 00:00:00
    secBuilder.append(1_609_545_600)  // 2021-01-02 00:00:00
    secBuilder.append(nil)
    #expect(secBuilder.nullCount == 1)
    #expect(secBuilder.length == 3)
    #expect(secBuilder.capacity == 256)
    let secArray = try secBuilder.finish()
    guard case .timestamp(let secType, let timezone) = secArray.arrowData.type
    else {
      Issue.record("Expected timestamp")
      return
    }
    #expect(secType == .second)
    #expect(timezone == nil)
    #expect(secArray.length == 3)
    #expect(secArray[0] == 1_609_459_200)
    #expect(secArray[1] == 1_609_545_600)
    #expect(secArray[2] == nil)

    // Test timestamp with milliseconds unit and timezone America/New_York
    let msBuilder = try ArrowArrayBuilders.loadTimestampArrayBuilder(
      .millisecond,
      timezone: "America/New_York"
    )
    msBuilder.append(1_609_459_200_000)  // 2021-01-01 00:00:00.000
    msBuilder.append(nil)
    msBuilder.append(1_609_545_600_000)  // 2021-01-02 00:00:00.000
    #expect(msBuilder.nullCount == 1)
    #expect(msBuilder.length == 3)
    #expect(msBuilder.capacity == 256)
    let msArray = try msBuilder.finish()
    guard case .timestamp(let msType, let timezone) = msArray.arrowData.type
    else {
      Issue.record("Expected timestamp")
      return
    }
    #expect(msType == .millisecond)
    #expect(timezone == "America/New_York")
    #expect(msArray.length == 3)
    #expect(msArray[0] == 1_609_459_200_000)
    #expect(msArray[1] == nil)
    #expect(msArray[2] == 1_609_545_600_000)

    // Test timestamp with microseconds unit and timezone UTC
    let usBuilder = try ArrowArrayBuilders.loadTimestampArrayBuilder(
      .microsecond, timezone: "UTC")
    usBuilder.append(1_609_459_200_000_000)  // 2021-01-01 00:00:00.000000
    usBuilder.append(1_609_545_600_000_000)  // 2021-01-02 00:00:00.000000
    usBuilder.append(1_609_632_000_000_000)  // 2021-01-03 00:00:00.000000
    #expect(usBuilder.nullCount == 0)
    #expect(usBuilder.length == 3)
    #expect(usBuilder.capacity == 256)
    let usArray = try usBuilder.finish()
    guard case .timestamp(let usType, let timezone) = usArray.arrowData.type
    else {
      Issue.record("Expected timestamp")
      return
    }
    #expect(usType == .microsecond)
    #expect(timezone == "UTC")
    #expect(usArray.length == 3)
    #expect(usArray[0] == 1_609_459_200_000_000)
    #expect(usArray[1] == 1_609_545_600_000_000)
    #expect(usArray[2] == 1_609_632_000_000_000)

    // Test timestamp with nanoseconds unit
    let nsBuilder = try ArrowArrayBuilders.loadTimestampArrayBuilder(
      .nanosecond, timezone: nil)
    nsBuilder.append(nil)
    // 2021-01-01 00:00:00.000000000
    nsBuilder.append(1_609_459_200_000_000_000)
    // 2021-01-02 00:00:00.000000000
    nsBuilder.append(1_609_545_600_000_000_000)
    #expect(nsBuilder.nullCount == 1)
    #expect(nsBuilder.length == 3)
    #expect(nsBuilder.capacity == 256)
    let nsArray = try nsBuilder.finish()
    guard case .timestamp(let nsType, let timezone) = nsArray.arrowData.type
    else {
      Issue.record("Expected timestamp")
      return
    }
    #expect(nsType == .nanosecond)
    #expect(timezone == nil)
    #expect(nsArray.length == 3)
    #expect(nsArray[0] == nil)
    #expect(nsArray[1] == 1_609_459_200_000_000_000)
    #expect(nsArray[2] == 1_609_545_600_000_000_000)
  }

  @Test func structArray() throws {
    class StructTest {
      var fieldBool: Bool = false
      var fieldInt8: Int8 = 0
      var fieldInt16: Int16 = 0
      var fieldInt32: Int32 = 0
      var fieldInt64: Int64 = 0
      var fieldUInt8: UInt8 = 0
      var fieldUInt16: UInt16 = 0
      var fieldUInt32: UInt32 = 0
      var fieldUInt64: UInt64 = 0
      var fieldDouble: Double = 0
      var fieldFloat: Float = 0
      var fieldString: String = ""
      var fieldData = Data()
      var fieldDate: Date = Date.now
    }

    enum STIndex: Int {
      case bool, int8, int16, int32, int64
      case uint8, uint16, uint32, uint64, double
      case float, string, data, date
    }

    let testData = StructTest()
    let dateNow = Date.now
    let structBuilder = try ArrowArrayBuilders.structArrayBuilderForType(
      testData)
    structBuilder.append([
      true, Int8(1), Int16(2), Int32(3), Int64(4),
      UInt8(5), UInt16(6), UInt32(7), UInt64(8), Double(9.9),
      Float(10.10), "11", Data("12".utf8), dateNow,
    ])
    structBuilder.append(nil)
    structBuilder.append([
      true, Int8(13), Int16(14), Int32(15), Int64(16),
      UInt8(17), UInt16(18), UInt32(19), UInt64(20), Double(21.21),
      Float(22.22), "23", Data("24".utf8), dateNow,
    ])
    #expect(structBuilder.length == 3)
    let structArray = try structBuilder.finish()
    #expect(structArray.length == 3)
    #expect(structArray[1] == nil)

    #expect(structArray.fields![0].length == 3)
    #expect(structArray.fields![0].asAny(1) == nil)
    #expect(structArray[0]![STIndex.bool.rawValue] as? Bool == true)
    #expect(structArray[0]![STIndex.int8.rawValue] as? Int8 == 1)
    #expect(structArray[0]![STIndex.int16.rawValue] as? Int16 == 2)
    #expect(structArray[0]![STIndex.int32.rawValue] as? Int32 == 3)
    #expect(structArray[0]![STIndex.int64.rawValue] as? Int64 == 4)
    #expect(structArray[0]![STIndex.uint8.rawValue] as? UInt8 == 5)
    #expect(structArray[0]![STIndex.uint16.rawValue] as? UInt16 == 6)
    #expect(structArray[0]![STIndex.uint32.rawValue] as? UInt32 == 7)
    #expect(structArray[0]![STIndex.uint64.rawValue] as? UInt64 == 8)
    #expect(structArray[0]![STIndex.double.rawValue] as? Double == 9.9)
    #expect(structArray[0]![STIndex.float.rawValue] as? Float == 10.10)
    #expect(structArray[2]![STIndex.string.rawValue] as? String == "23")
    #expect(
      String(
        decoding: (structArray[0]![STIndex.data.rawValue] as? Data)!,
        as: UTF8.self) == "12")
    let dateFormatter = DateFormatter()
    dateFormatter.timeStyle = .full
    #expect(
      dateFormatter.string(
        from: (structArray[0]![STIndex.date.rawValue] as? Date)!)
        == dateFormatter.string(from: dateNow))
  }

  func checkHolderForType(_ checkType: ArrowType) throws {
    let buffers = [
      ArrowBuffer(
        length: 0, capacity: 0,
        rawPointer: UnsafeMutableRawPointer.allocate(
          byteCount: 0, alignment: .zero)),
      ArrowBuffer(
        length: 0, capacity: 0,
        rawPointer: UnsafeMutableRawPointer.allocate(
          byteCount: 0, alignment: .zero)),
    ]
    let field = ArrowField(name: "", dataType: checkType, isNullable: true)
    switch makeArrayHolder(
      field, buffers: buffers, nullCount: 0, children: nil, rbLength: 0)
    {
    case .success(let holder):
      #expect(holder.type == checkType)
    case .failure(let err):
      throw err
    }
  }

  @Test func arrayHolders() throws {
    try checkHolderForType(.int8)
    try checkHolderForType(.uint8)
    try checkHolderForType(.int16)
    try checkHolderForType(.uint16)
    try checkHolderForType(.int32)
    try checkHolderForType(.uint32)
    try checkHolderForType(.int64)
    try checkHolderForType(.uint64)
    try checkHolderForType(.time32(.second))
    try checkHolderForType(.time32(.millisecond))
    try checkHolderForType(.time64(.microsecond))
    try checkHolderForType(.time64(.nanosecond))
    try checkHolderForType(.binary)
    try checkHolderForType(.float32)
    try checkHolderForType(.float64)
    try checkHolderForType(.boolean)
    try checkHolderForType(.utf8)
  }

  @Test func arrowArrayHolderBuilder() throws {
    let uint8HBuilder: AnyArrowArrayBuilder =
      (try ArrowArrayBuilders.loadNumberArrayBuilder()
        as NumberArrayBuilder<UInt8>)
    for index in 0..<100 {
      uint8HBuilder.appendAny(UInt8(index))
    }

    let uint8Holder = try uint8HBuilder.toAnyArrowArray()
    #expect(uint8Holder.nullCount == 0)
    #expect(uint8Holder.length == 100)

    let stringHBuilder: AnyArrowArrayBuilder =
      (try ArrowArrayBuilders.loadStringArrayBuilder())
    for index in 0..<100 {
      if index % 10 == 9 {
        stringHBuilder.appendAny(nil)
      } else {
        stringHBuilder.appendAny("test" + String(index))
      }
    }

    let stringHolder = try stringHBuilder.toAnyArrowArray()
    #expect(stringHolder.nullCount == 10)
    #expect(stringHolder.length == 100)
  }

  @Test func addVArgs() throws {
    let arrayBuilder: NumberArrayBuilder<UInt8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    arrayBuilder.append(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    #expect(arrayBuilder.length == 10)
    #expect(try arrayBuilder.finish()[2] == 2)
    let doubleBuilder: NumberArrayBuilder<Double> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    doubleBuilder.append(0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8)
    #expect(doubleBuilder.length == 9)
    #expect(try doubleBuilder.finish()[4] == 4.4)
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    stringBuilder.append("0", "1", "2", "3", "4", "5", "6")
    #expect(stringBuilder.length == 7)
    #expect(try stringBuilder.finish()[4] == "4")
    let boolBuilder = try ArrowArrayBuilders.loadBoolArrayBuilder()
    boolBuilder.append(true, false, true, false)
    #expect(try boolBuilder.finish()[2] == true)
  }

  @Test func addArray() throws {
    let arrayBuilder: NumberArrayBuilder<UInt8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    arrayBuilder.append([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    #expect(arrayBuilder.length == 10)
    #expect(try arrayBuilder.finish()[2] == 2)
    let doubleBuilder: NumberArrayBuilder<Double> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    doubleBuilder.append([0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8])
    #expect(doubleBuilder.length == 9)
    #expect(try doubleBuilder.finish()[4] == 4.4)
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    stringBuilder.append(["0", "1", "2", "3", "4", "5", "6"])
    #expect(stringBuilder.length == 7)
    #expect(try stringBuilder.finish()[4] == "4")
    let boolBuilder = try ArrowArrayBuilders.loadBoolArrayBuilder()
    boolBuilder.append([true, false, true, false])
    #expect(try boolBuilder.finish()[2] == true)
  }

  @Test func listArrayPrimitive() throws {
    let field = ArrowField(listFieldWith: .int32, isNullable: false)
    let listBuilder = try ListArrayBuilder(.list(field))

    listBuilder.append([Int32(1), Int32(2), Int32(3)])
    listBuilder.append([Int32(4), Int32(5)])
    listBuilder.append(nil)
    listBuilder.append([Int32(6), Int32(7), Int32(8), Int32(9)])

    #expect(listBuilder.length == 4)
    #expect(listBuilder.nullCount == 1)

    let listArray = try listBuilder.finish()
    #expect(listArray.length == 4)

    let firstList = listArray[0]
    #expect(firstList != nil, "First list should not be nil")
    #expect(firstList!.count == 3, "First list should have 3 elements")
    #expect(firstList![0] as? Int32 == 1)
    #expect(firstList![1] as? Int32 == 2)
    #expect(firstList![2] as? Int32 == 3)

    let secondList = listArray[1]
    #expect(secondList!.count == 2)
    #expect(secondList![0] as? Int32 == 4)
    #expect(secondList![1] as? Int32 == 5)

    #expect(listArray[2] == nil)

    let fourthList = listArray[3]
    #expect(fourthList!.count == 4)
    #expect(fourthList![0] as? Int32 == 6)
    #expect(fourthList![3] as? Int32 == 9)
  }

  @Test func listArrayNested() throws {
    let field = ArrowField(listFieldWith: .int32, isNullable: false)
    let innerListType: ArrowType = .list(field)
    let outerField = ArrowField(listFieldWith: innerListType, isNullable: false)
    let outerListBuilder = try ListArrayBuilder(.list(outerField))

    guard
      let innerListBuilder = outerListBuilder.valueBuilder as? ListArrayBuilder
    else {
      Issue.record("Failed to cast valueBuilder to ListArrayBuilder")
      return
    }

    outerListBuilder.bufferBuilder.append(2)
    innerListBuilder.append([Int32(1), Int32(2)])
    innerListBuilder.append([Int32(3), Int32(4), Int32(5)])

    outerListBuilder.bufferBuilder.append(1)
    innerListBuilder.append([Int32(6)])

    outerListBuilder.bufferBuilder.append(nil)

    outerListBuilder.bufferBuilder.append([])

    let nestedArray = try outerListBuilder.finish()
    #expect(nestedArray.length == 4)
    #expect(nestedArray.nullCount == 1)

    let firstOuterList = nestedArray[0]!
    #expect(firstOuterList.count == 2)

    let firstInnerList = firstOuterList[0] as! [Any?]
    #expect(firstInnerList.count == 2)
    #expect(firstInnerList[0] as? Int32 == 1)
    #expect(firstInnerList[1] as? Int32 == 2)

    let secondInnerList = firstOuterList[1] as! [Any?]
    #expect(secondInnerList.count == 3)
    #expect(secondInnerList[0] as? Int32 == 3)
    #expect(secondInnerList[1] as? Int32 == 4)
    #expect(secondInnerList[2] as? Int32 == 5)

    let secondOuterList = nestedArray[1]!
    #expect(secondOuterList.count == 1)

    let thirdInnerList = secondOuterList[0] as! [Any?]
    #expect(thirdInnerList.count == 1)
    #expect(thirdInnerList[0] as? Int32 == 6)

    #expect(nestedArray[2] == nil)

    let emptyList = nestedArray[3]!
    #expect(emptyList.count == 0)
  }
}
