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
      guard let stringColumn = recordBatch.arrays[1] as? ArrowArrayUtf8 else {
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
    let (arrowSchema, recordBatches) = try arrowReader.read()
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
}
