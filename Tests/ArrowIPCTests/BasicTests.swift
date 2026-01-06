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
@testable import ArrowIPC

struct BasicTests {

  @Test func boolFile() throws {
    let url = try loadTestResource(name: "testdata_bool")
    let arrowReader = try ArrowReader(url: url)
    let (_, recordBatches) = try arrowReader.read()
    for recordBatch in recordBatches {
      checkBoolRecordBatch(recordBatch: recordBatch)
    }
  }

  @Test func doubleFile() throws {

    let url = try loadTestResource(name: "testdata_double")
    let arrowReader = try ArrowReader(url: url)
    let (_, recordBatches) = try arrowReader.read()

    for recordBatch in recordBatches {

      // Test the Float64 column (index 0)
      guard
        let doubleColumn = recordBatch.arrays[0]
          as? ArrowArrayNumeric<Double>
      else {
        Issue.record("Failed to cast column 0 to ArrowArrayDouble")
        return
      }

      #expect(doubleColumn.length == 5)
      #expect(doubleColumn[0] == 1.1)
      #expect(doubleColumn[1] == 2.2)
      #expect(doubleColumn[2] == 3.3)
      #expect(doubleColumn[3] == 4.4)
      #expect(doubleColumn[4] == 5.5)

      // Test the String column (index 1)
      guard
        let stringColumn = recordBatch.arrays[1]
          as? ArrowArrayVariable<String, Int32>
      else {
        Issue.record("Failed to cast column 1 to ArrowArrayString")
        return
      }

      #expect(stringColumn.length == 5)
      #expect(stringColumn[0] == "zero")
      #expect(stringColumn[1] == nil)  // null value
      #expect(stringColumn[2] == "two")
      #expect(stringColumn[3] == "three")
      #expect(stringColumn[4] == "four")
    }
  }

  @Test func structFile() throws {
    let url = try loadTestResource(name: "testdata_struct")
    let arrowReader = try ArrowReader(url: url)
    let (_, recordBatches) = try arrowReader.read()
    for recordBatch in recordBatches {
      let structArray = try #require(
        recordBatch.arrays[0] as? ArrowStructArray)
      #expect(structArray.fields[0].name == "my string")
      #expect(structArray.fields[1].name == "my bool")
      #expect(structArray.length == 3)
      let row0 = try #require(structArray[0])
      #expect(row0["my string"] as? String == "0")
      #expect(row0["my bool"] as? Bool == false)
      let row1 = try #require(structArray[1])
      #expect(row1["my string"] as? String == "1")
      #expect(row1["my bool"] as? Bool == true)
      #expect(structArray[2] == nil)
      let stringArray = structArray.fields[0].array
      #expect(stringArray.length == 3)
      let boolArray = structArray.fields[1].array
      #expect(boolArray.length == 3)
    }
  }

  @Test func writeBasics() throws {

    let outputUrl = FileManager.default.temporaryDirectory
      .appending(path: "bool-test.arrow")
    let writer = ArrowWriter(url: outputUrl)
    #expect(writer.data.count == 8)

  }

  @Test func writeBoolean() throws {
    let schema: ArrowSchema = ArrowSchema.Builder()
      .addField("one", type: .boolean, isNullable: true)
      .addField("two", type: .utf8, isNullable: true)
      .finish()

    let builder = ArrayBuilderBoolean()
    builder.append(true)
    builder.append(false)
    builder.appendNull()
    builder.append(false)
    builder.append(true)
    let one = builder.finish()

    let builder2 = ArrayBuilderString()
    builder2.append("zero")
    builder2.append("one")
    builder2.append("two")
    builder2.append("three")
    builder2.append("four")
    let two = builder2.finish()

    let recordBatch = RecordBatch(schema: schema, columns: [one, two])

    checkBoolRecordBatch(recordBatch: recordBatch)

    let outputUrl = FileManager.default.temporaryDirectory
      .appending(path: "bool-test.arrow")
    var writer = ArrowWriter(url: outputUrl)
    try writer.write(schema: schema, recordBatches: [recordBatch])
    try writer.finish()

    let arrowReader = try ArrowReader(url: outputUrl)
    let (_, recordBatches) = try arrowReader.read()

    for recordBatch in recordBatches {
      checkBoolRecordBatch(recordBatch: recordBatch)
    }
    //    try FileManager.default.copyItem(at: outputUrl, to: URL(fileURLWithPath: "/tmp/bool-test-swift.arrow"))

  }
}
