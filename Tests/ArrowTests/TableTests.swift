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

import XCTest

@testable import Arrow

final class TableTests: XCTestCase {
  func testSchema() throws {
    let schemaBuilder = ArrowSchema.Builder()
    let schema = schemaBuilder.addField(
      "col1",
      type: .int8,
      isNullable: true
    )
    .addField("col2", type: .boolean, isNullable: false)
    .finish()
    XCTAssertEqual(schema.fields.count, 2)
    XCTAssertEqual(schema.fields[0].name, "col1")
    XCTAssertEqual(schema.fields[0].dataType, .int8)
    XCTAssertEqual(schema.fields[0].isNullable, true)
    XCTAssertEqual(schema.fields[1].name, "col2")
    XCTAssertEqual(schema.fields[1].dataType, .boolean)
    XCTAssertEqual(schema.fields[1].isNullable, false)
  }

  func testSchemaNested() throws {
    class StructTest {
      var field0: Bool = false
      var field1: Int8 = 0
      var field2: Int16 = 0
      var field3: Int32 = 0
      var field4: Int64 = 0
      var field5: UInt8 = 0
      var field6: UInt16 = 0
      var field7: UInt32 = 0
      var field8: UInt64 = 0
      var field9: Double = 0
      var field10: Float = 0
      var field11: String = ""
      var field12 = Data()
      var field13: Date = Date.now
    }

    let testObj = StructTest()
    var fields: [ArrowField] = []
    let buildStructType = { () -> ArrowType in
      let mirror = Mirror(reflecting: testObj)
      for (property, value) in mirror.children {
        let arrowType = try ArrowTypeConverter.infoForType(type(of: value))
        fields.append(
          ArrowField(
            name: property!,
            dataType: arrowType,
            isNullable: true
          )
        )
      }

      return .strct(fields)
    }

    let structType = try buildStructType()
    guard case .strct(let fields) = structType else {
      XCTFail("Expected a struct")
      return
    }
    XCTAssertEqual(fields.count, 14)
    XCTAssertEqual(fields[0].dataType, .boolean)
    XCTAssertEqual(fields[1].dataType, .int8)
    XCTAssertEqual(fields[2].dataType, .int16)
    XCTAssertEqual(fields[3].dataType, .int32)
    XCTAssertEqual(fields[4].dataType, .int64)
    XCTAssertEqual(fields[5].dataType, .uint8)
    XCTAssertEqual(fields[6].dataType, .uint16)
    XCTAssertEqual(fields[7].dataType, .uint32)
    XCTAssertEqual(fields[8].dataType, .uint64)
    XCTAssertEqual(fields[9].dataType, .float64)
    XCTAssertEqual(fields[10].dataType, .float32)
    XCTAssertEqual(fields[11].dataType, .utf8)
    XCTAssertEqual(fields[12].dataType, .binary)
    XCTAssertEqual(fields[13].dataType, .date64)
  }

  func testTable() throws {
    let doubleBuilder: NumberArrayBuilder<Double> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    doubleBuilder.append(11.11)
    doubleBuilder.append(22.22)
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    stringBuilder.append("test10")
    stringBuilder.append("test22")
    let date32Builder: Date32ArrayBuilder =
      try ArrowArrayBuilders.loadDate32ArrayBuilder()
    let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
    let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
    date32Builder.append(date1)
    date32Builder.append(date2)
    let table = try ArrowTable.Builder()
      .addColumn("col1", arrowArray: doubleBuilder.finish())
      .addColumn("col2", arrowArray: stringBuilder.finish())
      .addColumn("col3", arrowArray: date32Builder.finish())
      .finish()
    let schema = table.schema
    XCTAssertEqual(schema.fields.count, 3)
    XCTAssertEqual(schema.fields[0].name, "col1")
    XCTAssertEqual(schema.fields[0].dataType, .float64)
    XCTAssertEqual(schema.fields[0].isNullable, false)
    XCTAssertEqual(schema.fields[1].name, "col2")
    XCTAssertEqual(schema.fields[1].dataType, .utf8)
    XCTAssertEqual(schema.fields[1].isNullable, false)
    XCTAssertEqual(schema.fields[1].name, "col2")
    XCTAssertEqual(schema.fields[1].dataType, .utf8)
    XCTAssertEqual(schema.fields[1].isNullable, false)
    XCTAssertEqual(table.columns.count, 3)
    let col1: ChunkedArray<Double> = try table.columns[0].data()
    let col2: ChunkedArray<String> = try table.columns[1].data()
    let col3: ChunkedArray<Date> = try table.columns[2].data()
    XCTAssertEqual(col1.length, 2)
    XCTAssertEqual(col2.length, 2)
    XCTAssertEqual(col3.length, 2)
    XCTAssertEqual(col1[0], 11.11)
    XCTAssertEqual(col2[1], "test22")
  }

  func testTableWithChunkedData() throws {
    let uint8Builder: NumberArrayBuilder<UInt8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    uint8Builder.append(10)
    uint8Builder.append(22)
    let uint8Builder2: NumberArrayBuilder<UInt8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    uint8Builder2.append(33)
    let uint8Builder3: NumberArrayBuilder<UInt8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    uint8Builder3.append(44)
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    stringBuilder.append("test10")
    stringBuilder.append("test22")
    let stringBuilder2 = try ArrowArrayBuilders.loadStringArrayBuilder()
    stringBuilder.append("test33")
    stringBuilder.append("test44")
    let date32Builder: Date32ArrayBuilder =
      try ArrowArrayBuilders.loadDate32ArrayBuilder()
    let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
    let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
    date32Builder.append(date1)
    date32Builder.append(date2)
    date32Builder.append(date1)
    date32Builder.append(date2)
    let intArray = try ChunkedArray([
      uint8Builder.finish(), uint8Builder2.finish(), uint8Builder3.finish(),
    ])
    let stringArray = try ChunkedArray([
      stringBuilder.finish(), stringBuilder2.finish(),
    ])
    let dateArray = try ChunkedArray([date32Builder.finish()])
    let table = ArrowTable.Builder()
      .addColumn("col1", chunked: intArray)
      .addColumn("col2", chunked: stringArray)
      .addColumn("col3", chunked: dateArray)
      .finish()
    let schema = table.schema
    XCTAssertEqual(schema.fields.count, 3)
    XCTAssertEqual(schema.fields[0].name, "col1")
    XCTAssertEqual(schema.fields[0].dataType, .uint8)
    XCTAssertEqual(schema.fields[0].isNullable, false)
    XCTAssertEqual(schema.fields[1].name, "col2")
    XCTAssertEqual(schema.fields[1].dataType, .utf8)
    XCTAssertEqual(schema.fields[1].isNullable, false)
    XCTAssertEqual(schema.fields[1].name, "col2")
    XCTAssertEqual(schema.fields[1].dataType, .utf8)
    XCTAssertEqual(schema.fields[1].isNullable, false)
    XCTAssertEqual(table.columns.count, 3)
    let col1: ChunkedArray<UInt8> = try table.columns[0].data()
    let col2: ChunkedArray<String> = try table.columns[1].data()
    let col3: ChunkedArray<Date> = try table.columns[2].data()
    XCTAssertEqual(col1.length, 4)
    XCTAssertEqual(col2.length, 4)
    XCTAssertEqual(col3.length, 4)
    XCTAssertEqual(col1.asString(0), "10")
    XCTAssertEqual(col1.asString(3), "44")
    XCTAssertEqual(col2.asString(0), "test10")
    XCTAssertEqual(col2.asString(2), "test33")
  }

  func testTableToRecordBatch() throws {
    let uint8Builder: NumberArrayBuilder<UInt8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    uint8Builder.append(10)
    uint8Builder.append(22)
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    stringBuilder.append("test10")
    stringBuilder.append("test22")
    let intHolder = ArrowArrayHolderImpl(try uint8Builder.finish())
    let stringHolder = ArrowArrayHolderImpl(try stringBuilder.finish())
    let result = RecordBatch.Builder()
      .addColumn("col1", arrowArray: intHolder)
      .addColumn("col2", arrowArray: stringHolder)
      .finish().flatMap({ rb in
        ArrowTable.from(recordBatches: [rb])
      })
    switch result {
    case .success(let table):
      let schema = table.schema
      XCTAssertEqual(schema.fields.count, 2)
      XCTAssertEqual(schema.fields[0].name, "col1")
      XCTAssertEqual(schema.fields[0].dataType, .uint8)
      XCTAssertEqual(schema.fields[0].isNullable, false)
      XCTAssertEqual(schema.fields[1].name, "col2")
      XCTAssertEqual(schema.fields[1].dataType, .utf8)
      XCTAssertEqual(schema.fields[1].isNullable, false)
      XCTAssertEqual(table.columns.count, 2)
      let col1: ChunkedArray<UInt8> = try table.columns[0].data()
      let col2: ChunkedArray<String> = try table.columns[1].data()
      XCTAssertEqual(col1.length, 2)
      XCTAssertEqual(col2.length, 2)
    case .failure(let error):
      throw error
    }
  }
}
