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
import Foundation
import Testing

@testable import Arrow

let currentDate = Date.now

struct StructTest {
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

func loadArrowResource(name: String) throws(ArrowError) -> URL {
  if let resource = Bundle.module.url(
    forResource: name,
    withExtension: "arrow",
    subdirectory: "Resources"
  ) {
    return resource
  } else {
    throw .runtimeError("Couldn't find \(name).arrow in the test resources.")
  }
}

@discardableResult
func checkBoolRecordBatch(
  _ result: Result<ArrowReader.ArrowReaderResult, ArrowError>
) throws(ArrowError) -> [RecordBatch] {
  let recordBatches: [RecordBatch]
  switch result {
  case .success(let result):
    recordBatches = result.batches
  case .failure(let error):
    throw error
  }
  #expect(recordBatches.count == 1)
  for recordBatch in recordBatches {
    #expect(recordBatch.length == 5)
    #expect(recordBatch.columns.count == 2)
    #expect(recordBatch.schema.fields.count == 2)
    #expect(recordBatch.schema.fields[0].name == "one")
    #expect(recordBatch.schema.fields[0].type == .boolean)
    #expect(recordBatch.schema.fields[1].name == "two")
    #expect(recordBatch.schema.fields[1].type == .utf8)
    for index in 0..<recordBatch.length {
      let column = recordBatch.columns[0]
      //      guard let str = column as? AsString else {
      //        throw .invalid("Could not cast column to AsString")
      //      }
      let val = "\(column.asString(index))"
      if index == 0 || index == 4 {
        #expect(val == "true")
      } else if index == 2 {
        #expect(val.isEmpty)
      } else {
        #expect(val == "false")
      }
    }
  }
  return recordBatches
}

@discardableResult
func checkStructRecordBatch(
  _ result: Result<ArrowReader.ArrowReaderResult, ArrowError>
) throws(ArrowError) -> [RecordBatch] {
  let recordBatches: [RecordBatch]
  switch result {
  case .success(let result):
    recordBatches = result.batches
  case .failure(let error):
    throw error
  }
  #expect(recordBatches.count == 1)
  for recordBatch in recordBatches {
    #expect(recordBatch.length == 3)
    #expect(recordBatch.columns.count == 1)
    #expect(recordBatch.schema.fields.count == 1)
    #expect(recordBatch.schema.fields[0].name == "my struct")
    guard case .strct(_) = recordBatch.schema.fields[0].type else {
      Issue.record("Expected field 0 to be a struct")
      return []
    }
    guard let nestedArray = recordBatch.columns[0] as? NestedArray else {
      throw .runtimeError("Could not cast to NestedArray")
    }
    guard let fields = nestedArray.fields else {
      throw .runtimeError("NestedArray.fields is nil")
    }
    #expect(fields.count == 2)
    #expect(fields[0].type == .utf8)
    #expect(fields[1].type == .boolean)
    let column = recordBatch.columns[0]
    //    guard let str = column else {
    //      throw .runtimeError("String array is nil")
    //    }
    #expect("\(column.asString(0))" == "{0,false}")
    #expect("\(column.asString(1))" == "{1,true}")
    #expect(column.asAny(2) == nil)
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
      guard let property else {
        fatalError("Cannot get field name")
      }
      let arrowType = try ArrowTypeConverter.infoForType(type(of: value))
      fields.append(
        ArrowField(name: property, dataType: arrowType, isNullable: true))
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
  let structArray = try structBuilder.finish()
  let result = RecordBatch.Builder()
    .addColumn("struct1", arrowArray: structArray)
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

  let uint8Array = try uint8Builder.finish()
  let stringArray = try stringBuilder.finish()
  let date32Array = try date32Builder.finish()
  let int32Array = try int32Builder.finish()
  let floatArray = try floatBuilder.finish()
  let result = RecordBatch.Builder()
    .addColumn("col1", arrowArray: uint8Array)
    .addColumn("col2", arrowArray: stringArray)
    .addColumn("col3", arrowArray: date32Array)
    .addColumn("col4", arrowArray: int32Array)
    .addColumn("col5", arrowArray: floatArray)
    .finish()
  switch result {
  case .success(let recordBatch):
    return recordBatch
  case .failure(let error):
    throw error
  }
}

struct IPCStreamReaderTests {

  @Test func recordBatchInMemoryToFromStream() throws {
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
        #expect(recordBatches.count == 1)
        for recordBatch in recordBatches {
          #expect(recordBatch.length == 4)
          #expect(recordBatch.columns.count == 5)
          #expect(recordBatch.schema.fields.count == 5)
          #expect(recordBatch.schema.fields[0].name == "col1")
          #expect(recordBatch.schema.fields[0].type == .int8)
          #expect(recordBatch.schema.fields[1].name == "col2")
          #expect(recordBatch.schema.fields[1].type == .utf8)
          #expect(recordBatch.schema.fields[2].name == "col3")
          #expect(recordBatch.schema.fields[2].type == .date32)
          #expect(recordBatch.schema.fields[3].name == "col4")
          #expect(recordBatch.schema.fields[3].type == .int32)
          #expect(recordBatch.schema.fields[4].name == "col5")
          #expect(recordBatch.schema.fields[4].type == .float32)
          let columns = recordBatch.columns
          #expect(columns[0].nullCount == 2)
          let dateVal = "\((columns[2]).asString(0))"
          #expect(dateVal == "2014-09-10 00:00:00 +0000")
          let stringVal = "\((columns[1]).asString(1))"
          #expect(stringVal == "test22")
          let uintVal = "\((columns[0]).asString(0))"
          #expect(uintVal == "10")
          let stringVal2 = "\((columns[1]).asString(3))"
          #expect(stringVal2 == "test44")
          let uintVal2 = "\((columns[0]).asString(3))"
          #expect(uintVal2 == "44")
        }
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }
}

struct IPCFileReaderTests {
  @Test func fileReader_double() throws {
    let fileURL = try loadArrowResource(name: "testdata_double")
    let arrowReader = ArrowReader()
    let result = arrowReader.fromFile(fileURL)
    let recordBatches: [RecordBatch]
    switch result {
    case .success(let result):
      recordBatches = result.batches
    case .failure(let error):
      throw error
    }

    #expect(recordBatches.count == 1)
    for recordBatch in recordBatches {
      #expect(recordBatch.length == 5)
      #expect(recordBatch.columns.count == 2)
      #expect(recordBatch.schema.fields.count == 2)
      #expect(recordBatch.schema.fields[0].name == "one")
      #expect(
        recordBatch.schema.fields[0].type == .float64)
      #expect(recordBatch.schema.fields[1].name == "two")
      #expect(
        recordBatch.schema.fields[1].type == .utf8)
      for index in 0..<recordBatch.length {
        let column = recordBatch.columns[1]
        let val = "\(column.asString(index))"
        if index != 1 {
          #expect(!val.isEmpty)
        } else {
          #expect(val.isEmpty)
        }
      }
    }
  }

  @Test func fileReader_bool() throws {
    let fileURL = try loadArrowResource(name: "testdata_bool")
    let arrowReader = ArrowReader()
    try checkBoolRecordBatch(arrowReader.fromFile(fileURL))
  }

  @Test func fileWriter_bool() throws {
    // read existing file
    let fileURL = try loadArrowResource(name: "testdata_bool")
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

  @Test func fileReader_struct() throws {
    let fileURL = try loadArrowResource(name: "testdata_struct")
    let arrowReader = ArrowReader()
    try checkStructRecordBatch(arrowReader.fromFile(fileURL))
  }

  @Test func fileWriter_struct() throws {
    // read existing file
    let fileURL = try loadArrowResource(name: "testdata_struct")
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

  @Test func recordBatchInMemoryToFromStream() throws {
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
        #expect(recordBatches.count == 1)
        for recordBatch in recordBatches {
          #expect(recordBatch.length == 4)
          #expect(recordBatch.columns.count == 5)
          #expect(recordBatch.schema.fields.count == 5)
          #expect(recordBatch.schema.fields[0].name == "col1")
          #expect(recordBatch.schema.fields[0].type == .int8)
          #expect(recordBatch.schema.fields[1].name == "col2")
          #expect(recordBatch.schema.fields[1].type == .utf8)
          #expect(recordBatch.schema.fields[2].name == "col3")
          #expect(recordBatch.schema.fields[2].type == .date32)
          #expect(recordBatch.schema.fields[3].name == "col4")
          #expect(recordBatch.schema.fields[3].type == .int32)
          #expect(recordBatch.schema.fields[4].name == "col5")
          #expect(recordBatch.schema.fields[4].type == .float32)
          let columns = recordBatch.columns
          #expect(columns[0].nullCount == 2)
          let dateVal = "\(columns[2].asString(0))"
          #expect(dateVal == "2014-09-10 00:00:00 +0000")
          let stringVal = "\(columns[1].asString(1))"
          #expect(stringVal == "test22")
          let uintVal = "\(columns[0].asString(0))"
          #expect(uintVal == "10")
          let stringVal2 = "\(columns[1].asString(3))"
          #expect(stringVal2 == "test44")
          let uintVal2 = "\(columns[0].asString(3))"
          #expect(uintVal2 == "44")
        }
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }

  @Test func schemaInMemoryToFromStream() throws {
    // read existing file
    let schema = makeSchema()
    let arrowWriter = ArrowWriter()
    let writerInfo = ArrowWriter.Info(.schema, schema: schema)
    switch arrowWriter.writeFile(writerInfo) {
    case .success(let writeData):
      let arrowReader = ArrowReader()
      switch arrowReader.readFile(writeData) {
      case .success(let result):
        #expect(result.schema != nil)
        let schema = result.schema!
        #expect(schema.fields.count == 5)
        #expect(schema.fields[0].name == "col1")
        #expect(schema.fields[0].type == .int8)
        #expect(schema.fields[1].name == "col2")
        #expect(schema.fields[1].type == .utf8)
        #expect(schema.fields[2].name == "col3")
        #expect(schema.fields[2].type == .date32)
        #expect(schema.fields[3].name == "col4")
        #expect(schema.fields[3].type == .int32)
        #expect(schema.fields[4].name == "col5")
        #expect(schema.fields[4].type == .float32)
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

    let binaryArray = try binaryBuilder.finish()
    let result = RecordBatch.Builder()
      .addColumn("binary", arrowArray: binaryArray)
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
    let time64Array = try time64Builder.finish()
    let time32Array = try time32Builder.finish()
    let result = RecordBatch.Builder()
      .addColumn("time64", arrowArray: time64Array)
      .addColumn("time32", arrowArray: time32Array)
      .finish()
    switch result {
    case .success(let recordBatch):
      return (schema, recordBatch)
    case .failure(let error):
      throw error
    }
  }

  @Test func structRecordBatchInMemoryToFromStream() throws {
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
        #expect(recordBatches.count == 1)
        for recordBatch in recordBatches {
          #expect(recordBatch.length == 3)
          #expect(recordBatch.columns.count == 1)
          #expect(recordBatch.schema.fields.count == 1)
          #expect(recordBatch.schema.fields[0].name == "struct1")
          guard case .strct(let fields) = recordBatch.schema.fields[0].type
          else {
            Issue.record("Expected Struct")
            return
          }
          #expect(fields.count == 14)
          let columns = recordBatch.columns
          #expect(columns[0].nullCount == 1)
          #expect(columns[0].asAny(1) == nil)
          let structVal = "\(columns[0].asString(0))"
          #expect(
            structVal == "{true,1,2,3,4,5,6,7,8,9.9,10.1,11,12,\(currentDate)}")
          let nestedArray = (recordBatch.columns[0] as? NestedArray)!
          #expect(nestedArray.length == 3)
          #expect(nestedArray.fields != nil)
          #expect(nestedArray.fields!.count == 14)
          #expect(nestedArray.fields![0].type == .boolean)
          #expect(nestedArray.fields![1].type == .int8)
          #expect(nestedArray.fields![2].type == .int16)
          #expect(nestedArray.fields![3].type == .int32)
          #expect(nestedArray.fields![4].type == .int64)
          #expect(nestedArray.fields![5].type == .uint8)
          #expect(nestedArray.fields![6].type == .uint16)
          #expect(nestedArray.fields![7].type == .uint32)
          #expect(nestedArray.fields![8].type == .uint64)
          #expect(nestedArray.fields![9].type == .float64)
          #expect(nestedArray.fields![10].type == .float32)
          #expect(nestedArray.fields![11].type == .utf8)
          #expect(nestedArray.fields![12].type == .binary)
          #expect(nestedArray.fields![13].type == .date64)
        }
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }

  @Test func binaryInMemoryToFromStream() throws {
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
        #expect(result.schema != nil)
        let schema = result.schema!
        #expect(schema.fields.count == 1)
        #expect(schema.fields[0].name == "binary")
        #expect(schema.fields[0].type == .binary)
        #expect(result.batches.count == 1)
        let recordBatch = result.batches[0]
        #expect(recordBatch.length == 4)
        let columns = recordBatch.columns
        let stringVal =
          "\(columns[0].asString(1))"
        #expect(stringVal == "test22")
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }

  @Test func timeInMemoryToFromStream() throws {
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
        #expect(result.schema != nil)
        let schema = result.schema!
        #expect(schema.fields.count == 2)
        #expect(schema.fields[0].name == "time64")
        #expect(schema.fields[0].type == .time64(.microsecond))
        #expect(schema.fields[1].name == "time32")
        #expect(schema.fields[1].type == .time32(.millisecond))
        #expect(result.batches.count == 1)
        let recordBatch = result.batches[0]
        #expect(recordBatch.length == 4)
        let columns = recordBatch.columns
        let stringVal = "\(columns[0].asString(0))"
        #expect(stringVal == "12345678")
        let stringVal2 =
          "\(columns[1].asString(3))"
        #expect(stringVal2 == "3")
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }
}
