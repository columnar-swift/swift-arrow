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

import Arrow
import Testing

struct RecordBatchTests {

  @Test func recordBatch() throws {
    let uint8Builder: NumberArrayBuilder<UInt8> =
      try ArrowArrayBuilders.loadNumberArrayBuilder()
    uint8Builder.append(10)
    uint8Builder.append(22)
    uint8Builder.append(nil)
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    stringBuilder.append("test10")
    stringBuilder.append("test22")
    stringBuilder.append("test33")

    let intArray = try uint8Builder.finish()
    let stringArray = try stringBuilder.finish()
    let result = RecordBatchX.Builder()
      .addColumn("col1", arrowArray: intArray)
      .addColumn("col2", arrowArray: stringArray)
      .finish()
    switch result {
    case .success(let recordBatch):
      let schema = recordBatch.schema
      #expect(schema.fields.count == 2)
      #expect(schema.fields[0].name == "col1")
      #expect(schema.fields[0].type == .uint8)
      #expect(schema.fields[0].isNullable == true)
      #expect(schema.fields[1].name == "col2")
      #expect(schema.fields[1].type == .utf8)
      #expect(schema.fields[1].isNullable == false)
      #expect(recordBatch.columns.count == 2)
      let col1: any ArrowArray<UInt8> = try recordBatch.data(for: 0)
      let col2: any ArrowArray<String> = try recordBatch.data(for: 1)
      #expect(col1.length == 3)
      #expect(col2.length == 3)
      #expect(col1.nullCount == 1)
    case .failure(let error):
      throw error
    }
  }

  // Ensure that invalid record batches can't be built.
  @Test func schemaNullabilityChecked() throws {
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    stringBuilder.append("test10")
    stringBuilder.append(nil)
    stringBuilder.append("test33")
    let array = try stringBuilder.finish()

    let field = ArrowField(name: "col1", dataType: .utf8, isNullable: false)
    let result = RecordBatchX.Builder()
      .addColumn(field, arrowArray: array)
      .finish()
    if case .success(_) = result {
      Issue.record("Record batch should have rejected null data.")
    }
  }
}
