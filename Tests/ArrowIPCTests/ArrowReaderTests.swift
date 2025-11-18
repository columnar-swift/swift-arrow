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
import Testing

@testable import ArrowIPC

struct ArrowReaderTests {

  @Test func boolFile() throws {

    let url = try loadArrowResource(name: "testdata_bool")
    let arrowReader = try ArrowReader(url: url)
    let recordBatches = try arrowReader.read()

    for recordBatch in recordBatches {
      #expect(recordBatch.length == 5)
      #expect(recordBatch.columns.count == 2)
      #expect(recordBatch.schema.fields.count == 2)
      #expect(recordBatch.schema.fields[0].name == "one")
      #expect(recordBatch.schema.fields[0].type == .boolean)
      #expect(recordBatch.schema.fields[1].name == "two")
      #expect(recordBatch.schema.fields[1].type == .utf8)

      guard let booleanColumn = recordBatch.columns[0] as? ArrowArrayBoolean
      else {
        Issue.record("Failed to cast column to ArrowBooleanArray")
        return
      }
      #expect(booleanColumn[0] == true)
      #expect(booleanColumn[1] == false)
      #expect(booleanColumn[2] == nil)
      #expect(booleanColumn[3] == false)
      #expect(booleanColumn[4] == true)

      guard
        let utf8Column = recordBatch.columns[1] as? ArrowArrayUtf8
      else {
        Issue.record("Failed to cast column to ArrowUtf8Array")
        return
      }

      #expect(utf8Column[0] == "zero")
      #expect(utf8Column[1] == "one")
      #expect(utf8Column[2] == "two")
      #expect(utf8Column[3] == "three")
      #expect(utf8Column[4] == "four")
    }

  }

  @Test func doubleFile() throws {

    let url = try loadArrowResource(name: "testdata_double")
    let arrowReader = try ArrowReader(url: url)
    let recordBatches = try arrowReader.read()

    for recordBatch in recordBatches {

      // Test the Float64 column (index 0)
      guard
        let doubleColumn = recordBatch.columns[0]
          as? ArrowArrayFixed<Double, FixedWidthBufferIPC<Double>>
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
      guard let stringColumn = recordBatch.columns[1] as? ArrowArrayUtf8 else {
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
}
