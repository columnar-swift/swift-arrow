// Copyright 2025 The Apache Software Foundation
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

struct CodableTests {

  public class TestClass: Codable {
    public var propBool: Bool
    public var propInt8: Int8
    public var propInt16: Int16
    public var propInt32: Int32
    public var propInt64: Int64
    public var propUInt8: UInt8
    public var propUInt16: UInt16
    public var propUInt32: UInt32
    public var propUInt64: UInt64
    public var propFloat: Float
    public var propDouble: Double?
    public var propString: String
    public var propDate: Date

    public required init() {
      self.propBool = false
      self.propInt8 = 1
      self.propInt16 = 2
      self.propInt32 = 3
      self.propInt64 = 4
      self.propUInt8 = 5
      self.propUInt16 = 6
      self.propUInt32 = 7
      self.propUInt64 = 8
      self.propFloat = 9
      self.propDouble = 10
      self.propString = "11"
      self.propDate = Date.now
    }
  }

  @Test func arrowKeyedDecoder() throws {
    let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
    let boolBuilder = try ArrowArrayBuilders.loadBoolArrayBuilder()
    let int8Builder: NumberArrayBuilder<Int8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    let int16Builder: NumberArrayBuilder<Int16> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    let int32Builder: NumberArrayBuilder<Int32> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    let int64Builder: NumberArrayBuilder<Int64> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    let uint8Builder: NumberArrayBuilder<UInt8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    let uint16Builder: NumberArrayBuilder<UInt16> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    let uint32Builder: NumberArrayBuilder<UInt32> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    let uint64Builder: NumberArrayBuilder<UInt64> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    let floatBuilder: NumberArrayBuilder<Float> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    let doubleBuilder: NumberArrayBuilder<Double> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    let dateBuilder = try ArrowArrayBuilders.loadDate64ArrayBuilder()

    boolBuilder.append(false, true, false)
    int8Builder.append(10, 11, 12)
    int16Builder.append(20, 21, 22)
    int32Builder.append(30, 31, 32)
    int64Builder.append(40, 41, 42)
    uint8Builder.append(50, 51, 52)
    uint16Builder.append(60, 61, 62)
    uint32Builder.append(70, 71, 72)
    uint64Builder.append(80, 81, 82)
    floatBuilder.append(90.1, 91.1, 92.1)
    doubleBuilder.append(101.1, nil, nil)
    stringBuilder.append("test0", "test1", "test2")
    dateBuilder.append(date1, date1, date1)
    let result = RecordBatch.Builder()
      .addColumn("propBool", arrowArray: try boolBuilder.toHolder())
      .addColumn("propInt8", arrowArray: try int8Builder.toHolder())
      .addColumn("propInt16", arrowArray: try int16Builder.toHolder())
      .addColumn("propInt32", arrowArray: try int32Builder.toHolder())
      .addColumn("propInt64", arrowArray: try int64Builder.toHolder())
      .addColumn("propUInt8", arrowArray: try uint8Builder.toHolder())
      .addColumn("propUInt16", arrowArray: try uint16Builder.toHolder())
      .addColumn("propUInt32", arrowArray: try uint32Builder.toHolder())
      .addColumn("propUInt64", arrowArray: try uint64Builder.toHolder())
      .addColumn("propFloat", arrowArray: try floatBuilder.toHolder())
      .addColumn("propDouble", arrowArray: try doubleBuilder.toHolder())
      .addColumn("propString", arrowArray: try stringBuilder.toHolder())
      .addColumn("propDate", arrowArray: try dateBuilder.toHolder())
      .finish()
    switch result {
    case .success(let rb):
      let decoder = ArrowDecoder(rb)
      let testClasses = try decoder.decode(TestClass.self)
      for index in 0..<testClasses.count {
        let testClass = testClasses[index]
        #expect(testClass.propBool == (index % 2 == 0 ? false : true))
        #expect(testClass.propInt8 == Int8(index + 10))
        #expect(testClass.propInt16 == Int16(index + 20))
        #expect(testClass.propInt32 == Int32(index + 30))
        #expect(testClass.propInt64 == Int64(index + 40))
        #expect(testClass.propUInt8 == UInt8(index + 50))
        #expect(testClass.propUInt16 == UInt16(index + 60))
        #expect(testClass.propUInt32 == UInt32(index + 70))
        #expect(testClass.propUInt64 == UInt64(index + 80))
        #expect(testClass.propFloat == Float(index) + 90.1)
        if index == 0 {
          #expect(testClass.propDouble == 101.1)
        } else {
          #expect(testClass.propDouble == nil)
        }
        #expect(testClass.propString == "test\(index)")
        #expect(testClass.propDate == date1)
      }
    case .failure(let err):
      throw err
    }
  }

  @Test func arrowSingleDecoderWithoutNull() throws {
    let int8Builder: NumberArrayBuilder<Int8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    int8Builder.append(10, 11, 12)
    let result = RecordBatch.Builder()
      .addColumn("propInt8", arrowArray: try int8Builder.toHolder())
      .finish()
    switch result {
    case .success(let rb):
      let decoder = ArrowDecoder(rb)
      let testData = try decoder.decode(Int8?.self)
      for index in 0..<testData.count {
        let val: Int8? = testData[index]
        #expect(val! == Int8(index + 10))
      }
    case .failure(let err):
      throw err
    }
  }

  @Test func arrowSingleDecoderWithNull() throws {
    let int8WNilBuilder: NumberArrayBuilder<Int8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    int8WNilBuilder.append(10, nil, 12, nil)
    let resultWNil = RecordBatch.Builder()
      .addColumn("propInt8", arrowArray: try int8WNilBuilder.toHolder())
      .finish()
    switch resultWNil {
    case .success(let rb):
      let decoder = ArrowDecoder(rb)
      let testData = try decoder.decode(Int8?.self)
      for index in 0..<testData.count {
        let val: Int8? = testData[index]
        if index % 2 == 1 {
          #expect(val == nil)
        } else {
          #expect(val! == Int8(index + 10))
        }
      }
    case .failure(let err):
      throw err
    }
  }

  @Test func arrowMapDecoderWithoutNull() throws {
    let int8Builder: NumberArrayBuilder<Int8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    int8Builder.append(10, 11, 12, 13)
    stringBuilder.append("test10", "test11", "test12", "test13")
    switch RecordBatch.Builder()
      .addColumn("propInt8", arrowArray: try int8Builder.toHolder())
      .addColumn("propString", arrowArray: try stringBuilder.toHolder())
      .finish()
    {
    case .success(let rb):
      let decoder = ArrowDecoder(rb)
      let testData = try decoder.decode([Int8: String].self)
      for data in testData {
        #expect("test\(data.key)" == data.value)
      }
    case .failure(let err):
      throw err
    }

    switch RecordBatch.Builder()
      .addColumn("propString", arrowArray: try stringBuilder.toHolder())
      .addColumn("propInt8", arrowArray: try int8Builder.toHolder())
      .finish()
    {
    case .success(let rb):
      let decoder = ArrowDecoder(rb)
      let testData = try decoder.decode([String: Int8].self)
      for data in testData {
        #expect("test\(data.value)" == data.key)
      }
    case .failure(let err):
      throw err
    }
  }

  @Test func arrowMapDecoderWithNull() throws {
    let int8Builder: NumberArrayBuilder<Int8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    let stringWNilBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    int8Builder.append(10, 11, 12, 13)
    stringWNilBuilder.append(nil, "test11", nil, "test13")
    let resultWNil = RecordBatch.Builder()
      .addColumn("propInt8", arrowArray: try int8Builder.toHolder())
      .addColumn("propString", arrowArray: try stringWNilBuilder.toHolder())
      .finish()
    switch resultWNil {
    case .success(let rb):
      let decoder = ArrowDecoder(rb)
      let testData = try decoder.decode([Int8: String?].self)
      for data in testData {
        let str = data.value
        if data.key % 2 == 0 {
          #expect(str == nil)
        } else {
          #expect(str == "test\(data.key)")
        }
      }
    case .failure(let err):
      throw err
    }
  }

  func getArrayValue<T>(_ rb: RecordBatch, colIndex: Int, rowIndex: UInt) -> T?
  {
    let anyArray = rb.columns[colIndex].array
    return anyArray.asAny(UInt(rowIndex)) as? T
  }

  @Test func arrowKeyedEncoder() throws {
    var infos: [TestClass] = []
    for index in 0..<10 {
      let tClass = TestClass()
      let offset = index * 12
      tClass.propBool = index % 2 == 0
      tClass.propInt8 = Int8(offset + 1)
      tClass.propInt16 = Int16(offset + 2)
      tClass.propInt32 = Int32(offset + 3)
      tClass.propInt64 = Int64(offset + 4)
      tClass.propUInt8 = UInt8(offset + 5)
      tClass.propUInt16 = UInt16(offset + 6)
      tClass.propUInt32 = UInt32(offset + 7)
      tClass.propUInt64 = UInt64(offset + 8)
      tClass.propFloat = Float(offset + 9)
      tClass.propDouble = index % 2 == 0 ? Double(offset + 10) : nil
      tClass.propString = "\(offset + 11)"
      tClass.propDate = Date.now
      infos.append(tClass)
    }

    let rb = try ArrowEncoder.encode(infos)!
    #expect(Int(rb.length) == infos.count)
    #expect(rb.columns.count == 13)
    #expect(rb.columns[0].type == .boolean)
    #expect(rb.columns[1].type == .int8)
    #expect(rb.columns[2].type == .int16)
    #expect(rb.columns[3].type == .int32)
    #expect(rb.columns[4].type == .int64)
    #expect(rb.columns[5].type == .uint8)
    #expect(rb.columns[6].type == .uint16)
    #expect(rb.columns[7].type == .uint32)
    #expect(rb.columns[8].type == .uint64)
    #expect(rb.columns[9].type == .float32)
    #expect(rb.columns[10].type == .float64)
    #expect(rb.columns[11].type == .utf8)
    #expect(rb.columns[12].type == .date64)
    for index in 0..<10 {
      let offset = index * 12
      #expect(
        getArrayValue(rb, colIndex: 0, rowIndex: UInt(index))
          == (index % 2 == 0))
      #expect(
        getArrayValue(rb, colIndex: 1, rowIndex: UInt(index))
          == Int8(offset + 1))
      #expect(
        getArrayValue(rb, colIndex: 2, rowIndex: UInt(index))
          == Int16(offset + 2)
      )
      #expect(
        getArrayValue(rb, colIndex: 3, rowIndex: UInt(index))
          == Int32(offset + 3)
      )
      #expect(
        getArrayValue(rb, colIndex: 4, rowIndex: UInt(index))
          == Int64(offset + 4)
      )
      #expect(
        getArrayValue(rb, colIndex: 5, rowIndex: UInt(index))
          == UInt8(offset + 5)
      )
      #expect(
        getArrayValue(rb, colIndex: 6, rowIndex: UInt(index))
          == UInt16(offset + 6))
      #expect(
        getArrayValue(rb, colIndex: 7, rowIndex: UInt(index))
          == UInt32(offset + 7))
      #expect(
        getArrayValue(rb, colIndex: 8, rowIndex: UInt(index))
          == UInt64(offset + 8))
      #expect(
        getArrayValue(rb, colIndex: 9, rowIndex: UInt(index))
          == Float(offset + 9)
      )
      if index % 2 == 0 {
        #expect(
          getArrayValue(rb, colIndex: 10, rowIndex: UInt(index))
            == Double(offset + 10))
      } else {
        #expect(
          getArrayValue(rb, colIndex: 10, rowIndex: UInt(index)) == Double?(nil)
        )
      }
      #expect(
        getArrayValue(rb, colIndex: 11, rowIndex: UInt(index))
          == String(offset + 11))
    }
  }

  @Test func arrowUnkeyedEncoder() throws {
    var testMap: [Int8: String?] = [:]
    for index in 0..<10 {
      testMap[Int8(index)] = "test\(index)"
    }

    let rb = try ArrowEncoder.encode(testMap)
    #expect(Int(rb.length) == testMap.count)
    #expect(rb.columns.count == 2)
    #expect(rb.columns[0].type == .int8)
    #expect(rb.columns[1].type == .utf8)
    for index in 0..<10 {
      let key: Int8 = getArrayValue(rb, colIndex: 0, rowIndex: UInt(index))!
      let value: String = getArrayValue(rb, colIndex: 1, rowIndex: UInt(index))!
      #expect("test\(key)" == value)
    }
  }

  @Test func arrowSingleEncoder() throws {
    var intArray: [Int32?] = []
    for index in 0..<100 {
      if index == 10 {
        intArray.append(nil)
      } else {
        intArray.append(Int32(index))
      }
    }

    let rb = try ArrowEncoder.encode(intArray)!
    #expect(Int(rb.length) == intArray.count)
    #expect(rb.columns.count == 1)
    #expect(rb.columns[0].type == .int32)
    for index in 0..<100 {
      if index == 10 {
        let anyArray = rb.columns[0].array
        #expect(anyArray.asAny(UInt(index)) == nil)
      } else {
        #expect(
          getArrayValue(rb, colIndex: 0, rowIndex: UInt(index)) == Int32(index))
      }
    }
  }
}
