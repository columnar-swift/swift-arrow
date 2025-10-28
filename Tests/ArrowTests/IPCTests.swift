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

import FlatBuffers
import XCTest

@testable import Arrow

let currentDate = Date.now
class StructTest {
  var field0: Bool = false
  var field1: Int8 = 0
  var field2: Int16 = 0
  var field: Int32 = 0
  var field4: Int64 = 0
  var field5: UInt8 = 0
  var field6: UInt16 = 0
  var field7: UInt32 = 0
  var field8: UInt64 = 0
  var field9: Double = 0
  var field10: Float = 0
  var field11: String = ""
  var field12 = Data()
  var field13: Date = currentDate
}

func loadArrowResource(name: String) -> URL {
  Bundle.module.url(
    forResource: name,
    withExtension: "arrow",
    subdirectory: "Resources"
  )!
}

@discardableResult
func checkBoolRecordBatch(
  _ result: Result<ArrowReader.ArrowReaderResult, ArrowError>
) throws -> [RecordBatch] {
  let recordBatches: [RecordBatch]
  switch result {
  case .success(let result):
    recordBatches = result.batches
  case .failure(let error):
    throw error
  }
  XCTAssertEqual(recordBatches.count, 1)
  for recordBatch in recordBatches {
    XCTAssertEqual(recordBatch.length, 5)
    XCTAssertEqual(recordBatch.columns.count, 2)
    XCTAssertEqual(recordBatch.schema.fields.count, 2)
    XCTAssertEqual(recordBatch.schema.fields[0].name, "one")
    XCTAssertEqual(recordBatch.schema.fields[0].dataType, .boolean)
    XCTAssertEqual(recordBatch.schema.fields[1].name, "two")
    XCTAssertEqual(recordBatch.schema.fields[1].dataType, .utf8)
    for index in 0..<recordBatch.length {
      let column = recordBatch.columns[0]
      let str = column.array as! AsString
      let val = "\(str.asString(index))"
      if index == 0 || index == 4 {
        XCTAssertEqual(val, "true")
      } else if index == 2 {
        XCTAssertEqual(val, "")
      } else {
        XCTAssertEqual(val, "false")
      }
    }
  }
  return recordBatches
}

@discardableResult
func checkStructRecordBatch(
  _ result: Result<ArrowReader.ArrowReaderResult, ArrowError>
) throws -> [RecordBatch] {
  let recordBatches: [RecordBatch]
  switch result {
  case .success(let result):
    recordBatches = result.batches
  case .failure(let error):
    throw error
  }
  XCTAssertEqual(recordBatches.count, 1)
  for recordBatch in recordBatches {
    XCTAssertEqual(recordBatch.length, 3)
    XCTAssertEqual(recordBatch.columns.count, 1)
    XCTAssertEqual(recordBatch.schema.fields.count, 1)
    XCTAssertEqual(recordBatch.schema.fields[0].name, "my struct")
    guard case .strct(_) = recordBatch.schema.fields[0].dataType else {
      XCTFail("Expected field 0 to be a struct")
      return []
    }
    let nestedArray = recordBatch.columns[0].array as? NestedArray
    XCTAssertNotNil(nestedArray)
    XCTAssertNotNil(nestedArray!.fields)
    XCTAssertEqual(nestedArray!.fields!.count, 2)
    XCTAssertEqual(nestedArray!.fields![0].type, .utf8)
    XCTAssertEqual(nestedArray!.fields![1].type, .boolean)
    let column = recordBatch.columns[0]
    let str = column.array as? AsString
    XCTAssertEqual("\(str!.asString(0))", "{0,false}")
    XCTAssertEqual("\(str!.asString(1))", "{1,true}")
    XCTAssertTrue(column.array.asAny(2) == nil)
  }

  return recordBatches
}

func currentDirectory(path: String = #file) -> URL {
  URL(fileURLWithPath: path).deletingLastPathComponent()
}

func makeSchema() -> ArrowSchema {
  let schemaBuilder = ArrowSchema.Builder()
  return
    schemaBuilder
    .addField("col1", type: .int8, isNullable: true)
    .addField("col2", type: .utf8, isNullable: false)
    .addField("col3", type: .date32, isNullable: false)
    .addField("col4", type: .int32, isNullable: false)
    .addField("col5", type: .float32, isNullable: false)
    .finish()
}

func makeStructSchema() throws -> ArrowSchema {
  let testObj = StructTest()
  var fields: [ArrowField] = []
  let buildStructType = { () -> ArrowType in
    let mirror = Mirror(reflecting: testObj)
    for (property, value) in mirror.children {
      let arrowType = try ArrowTypeConverter.infoForType(type(of: value))
      fields.append(
        ArrowField(name: property!, dataType: arrowType, isNullable: true))
    }
    return .strct(fields)
  }

  return ArrowSchema.Builder()
    .addField("struct1", type: try buildStructType(), isNullable: true)
    .finish()
}

func makeStructRecordBatch() throws -> RecordBatch {
  let testData = StructTest()
  let dateNow = Date.now
  let structBuilder = try ArrowArrayBuilders.loadStructArrayBuilderForType(
    testData
  )
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
  let structHolder = ArrowArrayHolderImpl(try structBuilder.finish())
  let result = RecordBatch.Builder()
    .addColumn("struct1", arrowArray: structHolder)
    .finish()
  switch result {
  case .success(let recordBatch):
    return recordBatch
  case .failure(let error):
    throw error
  }
}

func makeRecordBatch() throws -> RecordBatch {
  let uint8Builder: NumberArrayBuilder<UInt8> =
    try ArrowArrayBuilders.loadNumberArrayBuilder()
  uint8Builder.append(10)
  uint8Builder.append(nil)
  uint8Builder.append(nil)
  uint8Builder.append(44)
  let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
  stringBuilder.append("test10")
  stringBuilder.append("test22")
  stringBuilder.append("test33")
  stringBuilder.append("test44")
  let date32Builder = try ArrowArrayBuilders.loadDate32ArrayBuilder()
  let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
  let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
  date32Builder.append(date1)
  date32Builder.append(date2)
  date32Builder.append(date1)
  date32Builder.append(date2)
  let int32Builder: NumberArrayBuilder<Int32> =
    try ArrowArrayBuilders.loadNumberArrayBuilder()
  int32Builder.append(1)
  int32Builder.append(2)
  int32Builder.append(3)
  int32Builder.append(4)
  let floatBuilder: NumberArrayBuilder<Float> =
    try ArrowArrayBuilders.loadNumberArrayBuilder()
  floatBuilder.append(211.112)
  floatBuilder.append(322.223)
  floatBuilder.append(433.334)
  floatBuilder.append(544.445)

  let uint8Holder = ArrowArrayHolderImpl(try uint8Builder.finish())
  let stringHolder = ArrowArrayHolderImpl(try stringBuilder.finish())
  let date32Holder = ArrowArrayHolderImpl(try date32Builder.finish())
  let int32Holder = ArrowArrayHolderImpl(try int32Builder.finish())
  let floatHolder = ArrowArrayHolderImpl(try floatBuilder.finish())
  let result = RecordBatch.Builder()
    .addColumn("col1", arrowArray: uint8Holder)
    .addColumn("col2", arrowArray: stringHolder)
    .addColumn("col3", arrowArray: date32Holder)
    .addColumn("col4", arrowArray: int32Holder)
    .addColumn("col5", arrowArray: floatHolder)
    .finish()
  switch result {
  case .success(let recordBatch):
    return recordBatch
  case .failure(let error):
    throw error
  }
}

final class IPCStreamReaderTests: XCTestCase {
  func testRBInMemoryToFromStream() throws {
    let schema = makeSchema()
    let recordBatch = try makeRecordBatch()
    let arrowWriter = ArrowWriter()
    let writerInfo = ArrowWriter.Info(
      .recordbatch, schema: schema, batches: [recordBatch])
    switch arrowWriter.writeStreaming(writerInfo) {
    case .success(let writeData):
      let arrowReader = ArrowReader()
      switch arrowReader.readStreaming(writeData) {
      case .success(let result):
        let recordBatches = result.batches
        XCTAssertEqual(recordBatches.count, 1)
        for recordBatch in recordBatches {
          XCTAssertEqual(recordBatch.length, 4)
          XCTAssertEqual(recordBatch.columns.count, 5)
          XCTAssertEqual(recordBatch.schema.fields.count, 5)
          XCTAssertEqual(recordBatch.schema.fields[0].name, "col1")
          XCTAssertEqual(recordBatch.schema.fields[0].dataType, .int8)
          XCTAssertEqual(recordBatch.schema.fields[1].name, "col2")
          XCTAssertEqual(recordBatch.schema.fields[1].dataType, .utf8)
          XCTAssertEqual(recordBatch.schema.fields[2].name, "col3")
          XCTAssertEqual(recordBatch.schema.fields[2].dataType, .date32)
          XCTAssertEqual(recordBatch.schema.fields[3].name, "col4")
          XCTAssertEqual(recordBatch.schema.fields[3].dataType, .int32)
          XCTAssertEqual(recordBatch.schema.fields[4].name, "col5")
          XCTAssertEqual(recordBatch.schema.fields[4].dataType, .float32)
          let columns = recordBatch.columns
          XCTAssertEqual(columns[0].nullCount, 2)
          let dateVal =
            "\((columns[2].array as! AsString).asString(0))"
          XCTAssertEqual(dateVal, "2014-09-10 00:00:00 +0000")
          let stringVal =
            "\((columns[1].array as! AsString).asString(1))"
          XCTAssertEqual(stringVal, "test22")
          let uintVal =
            "\((columns[0].array as! AsString).asString(0))"
          XCTAssertEqual(uintVal, "10")
          let stringVal2 =
            "\((columns[1].array as! AsString).asString(3))"
          XCTAssertEqual(stringVal2, "test44")
          let uintVal2 =
            "\((columns[0].array as! AsString).asString(3))"
          XCTAssertEqual(uintVal2, "44")
        }
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }
}

final class IPCFileReaderTests: XCTestCase {
  func testFileReader_double() throws {
    let fileURL = loadArrowResource(name: "testdata_double")
    let arrowReader = ArrowReader()
    let result = arrowReader.fromFile(fileURL)
    let recordBatches: [RecordBatch]
    switch result {
    case .success(let result):
      recordBatches = result.batches
    case .failure(let error):
      throw error
    }

    XCTAssertEqual(recordBatches.count, 1)
    for recordBatch in recordBatches {
      XCTAssertEqual(recordBatch.length, 5)
      XCTAssertEqual(recordBatch.columns.count, 2)
      XCTAssertEqual(recordBatch.schema.fields.count, 2)
      XCTAssertEqual(recordBatch.schema.fields[0].name, "one")
      XCTAssertEqual(
        recordBatch.schema.fields[0].dataType, .float64)
      XCTAssertEqual(recordBatch.schema.fields[1].name, "two")
      XCTAssertEqual(
        recordBatch.schema.fields[1].dataType, .utf8)
      for index in 0..<recordBatch.length {
        let column = recordBatch.columns[1]
        let str = column.array as! AsString
        let val = "\(str.asString(index))"
        if index != 1 {
          XCTAssertNotEqual(val, "")
        } else {
          XCTAssertEqual(val, "")
        }
      }
    }
  }

  func testFileReader_bool() throws {
    let fileURL = loadArrowResource(name: "testdata_bool")
    let arrowReader = ArrowReader()
    try checkBoolRecordBatch(arrowReader.fromFile(fileURL))
  }

  func testFileWriter_bool() throws {
    // read existing file
    let fileURL = loadArrowResource(name: "testdata_bool")
    let arrowReader = ArrowReader()
    let fileRBs = try checkBoolRecordBatch(arrowReader.fromFile(fileURL))
    let arrowWriter = ArrowWriter()
    // write data from file to a stream
    let writerInfo = ArrowWriter.Info(
      .recordbatch, schema: fileRBs[0].schema, batches: fileRBs)
    switch arrowWriter.writeFile(writerInfo) {
    case .success(let writeData):
      // read stream back into recordbatches
      try checkBoolRecordBatch(arrowReader.readFile(writeData))
    case .failure(let error):
      throw error
    }
    // write file record batches to another file
    let outputUrl = FileManager.default.temporaryDirectory
      .appending(path: "testfilewriter_bool.arrow")
    switch arrowWriter.toFile(outputUrl, info: writerInfo) {
    case .success:
      try checkBoolRecordBatch(arrowReader.fromFile(outputUrl))
    case .failure(let error):
      throw error
    }
  }

  func testFileReader_struct() throws {
    let fileURL = loadArrowResource(name: "testdata_struct")
    let arrowReader = ArrowReader()
    try checkStructRecordBatch(arrowReader.fromFile(fileURL))
  }

  func testFileWriter_struct() throws {
    // read existing file
    let fileURL = loadArrowResource(name: "testdata_struct")
    let arrowReader = ArrowReader()
    let fileRBs = try checkStructRecordBatch(arrowReader.fromFile(fileURL))
    let arrowWriter = ArrowWriter()
    // write data from file to a stream
    let writerInfo = ArrowWriter.Info(
      .recordbatch, schema: fileRBs[0].schema, batches: fileRBs)
    switch arrowWriter.writeFile(writerInfo) {
    case .success(let writeData):
      // read stream back into recordbatches
      try checkStructRecordBatch(arrowReader.readFile(writeData))
    case .failure(let error):
      throw error
    }
    // write file record batches to another file
    let outputUrl = FileManager.default.temporaryDirectory
      .appending(path: "testfilewriter_struct.arrow")
    switch arrowWriter.toFile(outputUrl, info: writerInfo) {
    case .success:
      try checkStructRecordBatch(arrowReader.fromFile(outputUrl))
    case .failure(let error):
      throw error
    }
  }

  func testRBInMemoryToFromStream() throws {
    // read existing file
    let schema = makeSchema()
    let recordBatch = try makeRecordBatch()
    let arrowWriter = ArrowWriter()
    let writerInfo = ArrowWriter.Info(
      .recordbatch, schema: schema, batches: [recordBatch])
    switch arrowWriter.writeFile(writerInfo) {
    case .success(let writeData):
      let arrowReader = ArrowReader()
      switch arrowReader.readFile(writeData) {
      case .success(let result):
        let recordBatches = result.batches
        XCTAssertEqual(recordBatches.count, 1)
        for recordBatch in recordBatches {
          XCTAssertEqual(recordBatch.length, 4)
          XCTAssertEqual(recordBatch.columns.count, 5)
          XCTAssertEqual(recordBatch.schema.fields.count, 5)
          XCTAssertEqual(recordBatch.schema.fields[0].name, "col1")
          XCTAssertEqual(recordBatch.schema.fields[0].dataType, .int8)
          XCTAssertEqual(recordBatch.schema.fields[1].name, "col2")
          XCTAssertEqual(recordBatch.schema.fields[1].dataType, .utf8)
          XCTAssertEqual(recordBatch.schema.fields[2].name, "col3")
          XCTAssertEqual(recordBatch.schema.fields[2].dataType, .date32)
          XCTAssertEqual(recordBatch.schema.fields[3].name, "col4")
          XCTAssertEqual(recordBatch.schema.fields[3].dataType, .int32)
          XCTAssertEqual(recordBatch.schema.fields[4].name, "col5")
          XCTAssertEqual(recordBatch.schema.fields[4].dataType, .float32)
          let columns = recordBatch.columns
          XCTAssertEqual(columns[0].nullCount, 2)
          let dateVal =
            "\((columns[2].array as! AsString).asString(0))"
          XCTAssertEqual(dateVal, "2014-09-10 00:00:00 +0000")
          let stringVal =
            "\((columns[1].array as! AsString).asString(1))"
          XCTAssertEqual(stringVal, "test22")
          let uintVal =
            "\((columns[0].array as! AsString).asString(0))"
          XCTAssertEqual(uintVal, "10")
          let stringVal2 =
            "\((columns[1].array as! AsString).asString(3))"
          XCTAssertEqual(stringVal2, "test44")
          let uintVal2 =
            "\((columns[0].array as! AsString).asString(3))"
          XCTAssertEqual(uintVal2, "44")
        }
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }

  func testSchemaInMemoryToFromStream() throws {
    // read existing file
    let schema = makeSchema()
    let arrowWriter = ArrowWriter()
    let writerInfo = ArrowWriter.Info(.schema, schema: schema)
    switch arrowWriter.writeFile(writerInfo) {
    case .success(let writeData):
      let arrowReader = ArrowReader()
      switch arrowReader.readFile(writeData) {
      case .success(let result):
        XCTAssertNotNil(result.schema)
        let schema = result.schema!
        XCTAssertEqual(schema.fields.count, 5)
        XCTAssertEqual(schema.fields[0].name, "col1")
        XCTAssertEqual(schema.fields[0].dataType, .int8)
        XCTAssertEqual(schema.fields[1].name, "col2")
        XCTAssertEqual(schema.fields[1].dataType, .utf8)
        XCTAssertEqual(schema.fields[2].name, "col3")
        XCTAssertEqual(schema.fields[2].dataType, .date32)
        XCTAssertEqual(schema.fields[3].name, "col4")
        XCTAssertEqual(schema.fields[3].dataType, .int32)
        XCTAssertEqual(schema.fields[4].name, "col5")
        XCTAssertEqual(schema.fields[4].dataType, .float32)
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }

  func makeBinaryDataset() throws -> (ArrowSchema, RecordBatch) {
    let schemaBuilder = ArrowSchema.Builder()
    let schema = schemaBuilder.addField(
      "binary", type: .binary, isNullable: false
    )
    .finish()

    let binaryBuilder = try ArrowArrayBuilders.loadBinaryArrayBuilder()
    binaryBuilder.append("test10".data(using: .utf8))
    binaryBuilder.append("test22".data(using: .utf8))
    binaryBuilder.append("test33".data(using: .utf8))
    binaryBuilder.append("test44".data(using: .utf8))

    let binaryHolder = ArrowArrayHolderImpl(try binaryBuilder.finish())
    let result = RecordBatch.Builder()
      .addColumn("binary", arrowArray: binaryHolder)
      .finish()
    switch result {
    case .success(let recordBatch):
      return (schema, recordBatch)
    case .failure(let error):
      throw error
    }
  }

  func makeTimeDataset() throws -> (ArrowSchema, RecordBatch) {
    let schemaBuilder = ArrowSchema.Builder()
    let schema = schemaBuilder.addField(
      "time64", type: .time64(.microsecond), isNullable: false
    )
    .addField("time32", type: .time32(.millisecond), isNullable: false)
    .finish()

    let time64Builder = try ArrowArrayBuilders.loadTime64ArrayBuilder(
      .nanosecond
    )
    time64Builder.append(12_345_678)
    time64Builder.append(1)
    time64Builder.append(nil)
    time64Builder.append(98_765_432)
    let time32Builder = try ArrowArrayBuilders.loadTime32ArrayBuilder(
      .millisecond
    )
    time32Builder.append(1)
    time32Builder.append(2)
    time32Builder.append(nil)
    time32Builder.append(3)
    let time64Holder = ArrowArrayHolderImpl(try time64Builder.finish())
    let time32Holder = ArrowArrayHolderImpl(try time32Builder.finish())
    let result = RecordBatch.Builder()
      .addColumn("time64", arrowArray: time64Holder)
      .addColumn("time32", arrowArray: time32Holder)
      .finish()
    switch result {
    case .success(let recordBatch):
      return (schema, recordBatch)
    case .failure(let error):
      throw error
    }
  }

  func testStructRBInMemoryToFromStream() throws {
    // read existing file
    let schema = try makeStructSchema()
    let recordBatch = try makeStructRecordBatch()
    let arrowWriter = ArrowWriter()
    let writerInfo = ArrowWriter.Info(
      .recordbatch,
      schema: schema,
      batches: [recordBatch]
    )
    switch arrowWriter.writeStreaming(writerInfo) {
    case .success(let writeData):
      let arrowReader = ArrowReader()
      switch arrowReader.readStreaming(writeData) {
      case .success(let result):
        let recordBatches = result.batches
        XCTAssertEqual(recordBatches.count, 1)
        for recordBatch in recordBatches {
          XCTAssertEqual(recordBatch.length, 3)
          XCTAssertEqual(recordBatch.columns.count, 1)
          XCTAssertEqual(recordBatch.schema.fields.count, 1)
          XCTAssertEqual(recordBatch.schema.fields[0].name, "struct1")
          guard case .strct(let fields) = recordBatch.schema.fields[0].dataType
          else {
            XCTFail("Expected Struct")
            return
          }
          XCTAssertEqual(fields.count, 14)
          let columns = recordBatch.columns
          XCTAssertEqual(columns[0].nullCount, 1)
          XCTAssertNil(columns[0].array.asAny(1))
          let structVal = "\((columns[0].array as? AsString)!.asString(0))"
          XCTAssertEqual(
            structVal, "{true,1,2,3,4,5,6,7,8,9.9,10.1,11,12,\(currentDate)}")
          let nestedArray = (recordBatch.columns[0].array as? NestedArray)!
          XCTAssertEqual(nestedArray.length, 3)
          XCTAssertNotNil(nestedArray.fields)
          XCTAssertEqual(nestedArray.fields!.count, 14)
          XCTAssertEqual(nestedArray.fields![0].type, .boolean)
          XCTAssertEqual(nestedArray.fields![1].type, .int8)
          XCTAssertEqual(nestedArray.fields![2].type, .int16)
          XCTAssertEqual(nestedArray.fields![3].type, .int32)
          XCTAssertEqual(nestedArray.fields![4].type, .int64)
          XCTAssertEqual(nestedArray.fields![5].type, .uint8)
          XCTAssertEqual(nestedArray.fields![6].type, .uint16)
          XCTAssertEqual(nestedArray.fields![7].type, .uint32)
          XCTAssertEqual(nestedArray.fields![8].type, .uint64)
          XCTAssertEqual(nestedArray.fields![9].type, .float64)
          XCTAssertEqual(nestedArray.fields![10].type, .float32)
          XCTAssertEqual(nestedArray.fields![11].type, .utf8)
          XCTAssertEqual(nestedArray.fields![12].type, .binary)
          XCTAssertEqual(nestedArray.fields![13].type, .date64)
        }
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }

  func testBinaryInMemoryToFromStream() throws {
    let dataset = try makeBinaryDataset()
    let writerInfo = ArrowWriter.Info(
      .recordbatch,
      schema: dataset.0,
      batches: [dataset.1]
    )
    let arrowWriter = ArrowWriter()
    switch arrowWriter.writeFile(writerInfo) {
    case .success(let writeData):
      let arrowReader = ArrowReader()
      switch arrowReader.readFile(writeData) {
      case .success(let result):
        XCTAssertNotNil(result.schema)
        let schema = result.schema!
        XCTAssertEqual(schema.fields.count, 1)
        XCTAssertEqual(schema.fields[0].name, "binary")
        XCTAssertEqual(schema.fields[0].dataType, .binary)
        XCTAssertEqual(result.batches.count, 1)
        let recordBatch = result.batches[0]
        XCTAssertEqual(recordBatch.length, 4)
        let columns = recordBatch.columns
        let stringVal =
          "\((columns[0].array as! AsString).asString(1))"
        XCTAssertEqual(stringVal, "test22")
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }

  func testTimeInMemoryToFromStream() throws {
    let dataset = try makeTimeDataset()
    let writerInfo = ArrowWriter.Info(
      .recordbatch,
      schema: dataset.0,
      batches: [dataset.1]
    )
    let arrowWriter = ArrowWriter()
    switch arrowWriter.writeFile(writerInfo) {
    case .success(let writeData):
      let arrowReader = ArrowReader()
      switch arrowReader.readFile(writeData) {
      case .success(let result):
        XCTAssertNotNil(result.schema)
        let schema = result.schema!
        XCTAssertEqual(schema.fields.count, 2)
        XCTAssertEqual(schema.fields[0].name, "time64")
        XCTAssertEqual(schema.fields[0].dataType, .time64(.microsecond))
        XCTAssertEqual(schema.fields[1].name, "time32")
        XCTAssertEqual(schema.fields[1].dataType, .time32(.millisecond))
        XCTAssertEqual(result.batches.count, 1)
        let recordBatch = result.batches[0]
        XCTAssertEqual(recordBatch.length, 4)
        let columns = recordBatch.columns
        let stringVal =
          "\((columns[0].array as! AsString).asString(0))"
        XCTAssertEqual(stringVal, "12345678")
        let stringVal2 =
          "\((columns[1].array as! AsString).asString(3))"
        XCTAssertEqual(stringVal2, "3")
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }
}
