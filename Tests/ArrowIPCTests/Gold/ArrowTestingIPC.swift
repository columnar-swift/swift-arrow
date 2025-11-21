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

struct ArrowTestingIPC {

  @Test func generatedBinary() throws {

    guard
      let resourceURL = Bundle.module.url(
        forResource: "Resources/integration/cpp-21.0.0/generated_binary.json",
        withExtension: "lz4"
      )
    else {
      throw ArrowError.invalid("Unable to locate generated_binary.json")
    }

    let lz4Data = try Data(contentsOf: resourceURL)
    let lz4 = try LZ4(parsing: lz4Data)
    let testCase = try JSONDecoder().decode(
      ArrowTestingFormat.self, from: lz4.data)

    //    try printTestJSON(testCase)

    guard
      let testFile = Bundle.module.url(
        forResource: "Resources/integration/cpp-21.0.0/generated_binary",
        withExtension: "arrow_file"
      )
    else {
      throw ArrowError.invalid("Unable to locate arrow file.")
    }

    let arrowReader = try ArrowReader(url: testFile)
    let (arrowSchema, recordBatches) = try arrowReader.read()

    #expect(testCase.batches.count == recordBatches.count)

    for (testBatch, recordBatch) in zip(testCase.batches, recordBatches) {
      for ((expectedField, expectedColumn), (arrowField, arrowArray)) in zip(
        zip(testCase.schema.fields, testBatch.columns),
        zip(arrowSchema.fields, recordBatch.arrays)
      ) {
        #expect(arrowArray.length == expectedColumn.count)
        #expect(arrowField.name == expectedColumn.name)

        switch arrowField.type {
        case .fixedSizeBinary(let byteWidth):
          guard let expectedByteWidth = expectedField.type.byteWidth else {
            throw ArrowError.invalid(
              "Test case is missing byteWidth for fixedSizeBinary field."
            )
          }
          #expect(expectedByteWidth == byteWidth)
          guard let actual = arrowArray as? ArrowArrayOfData else {
            Issue.record(
              "Expected ArrowArrayOfData but got \(type(of: arrowArray))"
            )
            continue
          }
          try testFixedWidthBinary(actual: actual, expected: expectedColumn)
        case .binary:
          try testVariable(
            actual: arrowArray, expected: expectedColumn, type: arrowField.type)
        case .utf8:
          try testVariable(
            actual: arrowArray, expected: expectedColumn, type: arrowField.type)
        default:
          print(arrowField.type)
          throw ArrowError.notImplemented
        }
      }
    }
  }

  func testFixedWidthBinary(
    actual: ArrowArrayOfData,
    expected: ArrowTestingFormat.Column,
  ) throws {
    guard let validity = expected.validity, let dataValues = expected.data
    else {
      throw ArrowError.invalid("Test column is incomplete.")
    }
    for (i, isNull) in validity.enumerated() {
      let hex = dataValues[i]
      guard let data = Data(hex: hex) else {
        Issue.record("Failed to decode data from hex: \(hex)")
        return
      }
      if isNull == 0 {
        #expect(actual[i] == nil)
      } else {
        #expect(actual[i] == data)
      }
    }
  }

  func testVariable(
    actual: AnyArrowArrayProtocol,
    expected: ArrowTestingFormat.Column,
    type: ArrowType
  ) throws {
    guard let expectedValidity = expected.validity,
      let expectedOffsets = expected.offset,
      let expectedValues = expected.data
    else {
      throw ArrowError.invalid("Test column is incomplete.")
    }

    actual.buffers[1].withUnsafeBytes { ptr in
      let offsets = ptr.bindMemory(to: Int32.self)
      for (i, expectedOffset) in expectedOffsets.enumerated() {
        #expect(offsets[i] == expectedOffset)
      }
    }
    switch type {
    case .binary:
      guard let binaryArray = actual as? ArrowArrayOfData else {
        Issue.record("Binary array expected.")
        return
      }
      for i in 0..<expected.count {
        let hex = expectedValues[i]
        guard let expectedData = Data(hex: hex) else {
          Issue.record("Failed to decode data from hex: \(hex)")
          return
        }
        if expectedValidity[i] == 0 {
          #expect(binaryArray[i] == nil)
        } else {
          #expect(binaryArray[i] == expectedData)
        }
      }
    case .utf8:
      guard let binaryArray = actual as? ArrowArrayOfString else {
        Issue.record("Binary array expected.")
        return
      }
      for i in 0..<expected.count {
        let utf8 = expectedValues[i]
        if expectedValidity[i] == 0 {
          #expect(binaryArray[i] == nil)
        } else {
          #expect(binaryArray[i] == utf8)
        }
      }
    default:
      Issue.record("Unhandled type: \(type)")
    }
  }

}
