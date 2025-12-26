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

struct TableTests {

  @Test func schema() throws {
    let schemaBuilder = ArrowSchema.Builder()
    let schema = schemaBuilder.addField(
      "col1",
      type: .int8,
      isNullable: true
    )
    .addField("col2", type: .boolean, isNullable: false)
    .finish()
    #expect(schema.fields.count == 2)
    #expect(schema.fields[0].name == "col1")
    #expect(schema.fields[0].type == .int8)
    #expect(schema.fields[0].isNullable == true)
    #expect(schema.fields[1].name == "col2")
    #expect(schema.fields[1].type == .boolean)
    #expect(schema.fields[1].isNullable == false)
  }

  @Test func table() throws {
    let doubleBuilder: ArrayBuilderFixedWidth<Double> = .init()
    doubleBuilder.append(11.11)
    doubleBuilder.append(22.22)
    let stringBuilder = ArrayBuilderVariableLength<String, Int32>()
    stringBuilder.append("test10")
    stringBuilder.append("test22")
    let date32Builder: ArrayBuilderDate32 = .init()
    let date2 = Date(timeIntervalSinceReferenceDate: 86400 * 1)
    let date1 = Date(timeIntervalSinceReferenceDate: 86400 * 5000 + 352)
    date32Builder.append(date1)
    date32Builder.append(date2)
    let table = try ArrowTable.Builder()
      .addColumn(
        ArrowField(name: "col1", dataType: .float64, isNullable: false),
        arrowArray: doubleBuilder.finish()
      )
      .addColumn(
        ArrowField(name: "col2", dataType: .utf8, isNullable: false),
        arrowArray: stringBuilder.finish()
      )
      .addColumn(
        ArrowField(name: "col3", dataType: .date32, isNullable: false),
        arrowArray: date32Builder.finish()
      )
      .finish()
    let schema = table.schema
    #expect(schema.fields.count == 3)
    #expect(schema.fields[0].name == "col1")
    #expect(schema.fields[0].type == .float64)
    #expect(schema.fields[0].isNullable == false)
    #expect(schema.fields[1].name == "col2")
    #expect(schema.fields[1].type == .utf8)
    #expect(schema.fields[1].isNullable == false)
    #expect(schema.fields[1].name == "col2")
    #expect(schema.fields[1].type == .utf8)
    #expect(schema.fields[1].isNullable == false)
    #expect(table.columns.count == 3)
    let col1: ChunkedArray<Double> = try table.columns[0].data()
    let col2: ChunkedArray<String> = try table.columns[1].data()
    let col3: ChunkedArray<Date> = try table.columns[2].data()
    #expect(col1.length == 2)
    #expect(col2.length == 2)
    #expect(col3.length == 2)
    #expect(col1[0] == 11.11)
    #expect(col2[1] == "test22")
  }

  @Test func tableWithChunkedData() throws {
    let uint8Builder: ArrayBuilderFixedWidth<UInt8> = .init()
    uint8Builder.append(10)
    uint8Builder.append(22)
    let uint8Builder2: ArrayBuilderFixedWidth<UInt8> = .init()
    uint8Builder2.append(33)
    let uint8Builder3: ArrayBuilderFixedWidth<UInt8> = .init()
    uint8Builder3.append(44)
    let stringBuilder = ArrayBuilderVariableLength<String, Int32>()
    stringBuilder.append("test10")
    stringBuilder.append("test22")
    let stringBuilder2 = ArrayBuilderVariableLength<String, Int32>()
    stringBuilder2.append("test33")
    stringBuilder2.append("test44")
    let date32Builder: ArrayBuilderDate32 = .init()
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
      .addColumn(
        ArrowField(name: "col1", dataType: .uint8, isNullable: false),
        chunked: intArray
      )
      .addColumn(
        ArrowField(name: "col2", dataType: .utf8, isNullable: false),
        chunked: stringArray
      )
      .addColumn(
        ArrowField(name: "col3", dataType: .date32, isNullable: false),
        chunked: dateArray
      )
      .finish()
    let schema = table.schema
    #expect(schema.fields.count == 3)
    #expect(schema.fields[0].name == "col1")
    #expect(schema.fields[0].type == .uint8)
    #expect(schema.fields[0].isNullable == false)
    #expect(schema.fields[1].name == "col2")
    #expect(schema.fields[1].type == .utf8)
    #expect(schema.fields[1].isNullable == false)
    #expect(schema.fields[1].name == "col2")
    #expect(schema.fields[1].type == .utf8)
    #expect(schema.fields[1].isNullable == false)
    #expect(table.columns.count == 3)
    let col1: ChunkedArray<UInt8> = try table.columns[0].data()
    let col2: ChunkedArray<String> = try table.columns[1].data()
    let col3: ChunkedArray<Date> = try table.columns[2].data()
    #expect(col1.length == 4)
    #expect(col2.length == 4)
    #expect(col3.length == 4)
    #expect(col1.asString(0) == "10")
    #expect(col1.asString(3) == "44")
    #expect(col2.asString(0) == "test10")
    #expect(col2.asString(2) == "test33")
  }

//  @Test func tableToRecordBatch() throws {
//    let uint8Builder: NumberArrayBuilder<UInt8> =
//      try ArrowArrayBuilders.loadNumberArrayBuilder()
//    uint8Builder.append(10)
//    uint8Builder.append(22)
//    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
//    stringBuilder.append("test10")
//    stringBuilder.append("test22")
//    let intArray = try uint8Builder.finish()
//    let stringArray = try stringBuilder.finish()
//    let result = RecordBatchX.Builder()
//      .addColumn("col1", arrowArray: intArray)
//      .addColumn("col2", arrowArray: stringArray)
//      .finish().flatMap({ rb in
//        ArrowTable.from(recordBatches: [rb])
//      })
//    switch result {
//    case .success(let table):
//      let schema = table.schema
//      #expect(schema.fields.count == 2)
//      #expect(schema.fields[0].name == "col1")
//      #expect(schema.fields[0].type == .uint8)
//      #expect(schema.fields[0].isNullable == false)
//      #expect(schema.fields[1].name == "col2")
//      #expect(schema.fields[1].type == .utf8)
//      #expect(schema.fields[1].isNullable == false)
//      #expect(table.columns.count == 2)
//      let col1: ChunkedArray<UInt8> = try table.columns[0].data()
//      let col2: ChunkedArray<String> = try table.columns[1].data()
//      #expect(col1.length == 2)
//      #expect(col2.length == 2)
//    case .failure(let error):
//      throw error
//    }
//  }
}
