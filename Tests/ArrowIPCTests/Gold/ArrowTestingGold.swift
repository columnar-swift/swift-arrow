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

  static let allTests = [
    "generated_binary",
    "generated_binary_no_batches",
    "generated_binary_view",
    "generated_binary_zerolength",
    "generated_custom_metadata",
    "generated_datetime",
    "generated_decimal",
    "generated_decimal256",
    "generated_decimal32",
    "generated_decimal64",
    "generated_dictionary",
    "generated_dictionary_unsigned",
    "generated_duplicate_fieldnames",
    "generated_duration",
    "generated_extension",
    "generated_interval",
    "generated_interval_mdn",
    "generated_large_binary",
    "generated_list_view",
    "generated_map",
    "generated_map_non_canonical",
    "generated_nested",
    "generated_nested_dictionary",
    "generated_nested_large_offsets",
    "generated_null",
    "generated_null_trivial",
    "generated_primitive",
    "generated_primitive_no_batches",
    "generated_primitive_zerolength",
    "generated_recursive_nested",
    "generated_run_end_encoded",
    "generated_union",
  ]

  static let testCases: [String] = [
    "generated_primitive",
    "generated_primitive_no_batches",
    "generated_primitive_zerolength",
    "generated_binary",
    "generated_binary_zerolength",
    "generated_binary_no_batches",
    "generated_custom_metadata",
    "generated_nested",
  ]

  //  @Test(.serialized, arguments: testCases)
  @Test(arguments: testCases)
  func gold(name: String) throws {

    //    print(name)
    //    print(Self.testCases)

    //    let todos = Set(Self.allTests).subtracting(Set(Self.testCases))
    //    for todo in todos.sorted() {
    //      print(todo)
    //    }

    let resourceURL = try loadTestResource(
      name: name,
      withExtension: "json.lz4",
      subdirectory: "integration/cpp-21.0.0"
    )
    let lz4Data = try Data(contentsOf: resourceURL)
    let lz4 = try LZ4(parsing: lz4Data)
    let testCase = try JSONDecoder().decode(ArrowGold.self, from: lz4.data)
    let testFile = try loadTestResource(
      name: name,
      withExtension: "arrow_file",
      subdirectory: "integration/cpp-21.0.0"
    )
    let arrowReader = try ArrowReader(url: testFile)
    let (arrowSchema, recordBatches) = try arrowReader.read()

    #expect(testCase.batches.count == recordBatches.count)

    let expectedMetadata = testCase.schema.metadata?.asDictionary ?? [:]
    #expect(expectedMetadata == arrowSchema.metadata)

    for (testBatch, recordBatch) in zip(testCase.batches, recordBatches) {
      for (
        (arrowField, arrowArray),
        (expectedField, expectedColumn)
      ) in zip(
        zip(arrowSchema.fields, recordBatch.arrays),
        zip(testCase.schema.fields, testBatch.columns)
      ) {

        #expect(arrowField.name == expectedField.name)
        #expect(arrowField.isNullable == expectedField.nullable)
        #expect(arrowField.type.matches(expectedField: expectedField))
        #expect(arrowArray.length == expectedColumn.count)
        #expect(arrowField.name == expectedColumn.name)
        let expectedMetadata = expectedField.metadata?.asDictionary ?? [:]
        #expect(arrowField.metadata == expectedMetadata)

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
        case .boolean:
          try testBoolean(actual: arrowArray, expected: expectedColumn)
        case .int8:
          try testFixedWidth(
            actual: arrowArray, expected: expectedColumn, as: Int8.self)
        case .uint8:
          try testFixedWidth(
            actual: arrowArray, expected: expectedColumn, as: UInt8.self)
        case .int16:
          try testFixedWidth(
            actual: arrowArray, expected: expectedColumn, as: Int16.self)
        case .uint16:
          try testFixedWidth(
            actual: arrowArray, expected: expectedColumn, as: UInt16.self)
        case .int32:
          try testFixedWidth(
            actual: arrowArray, expected: expectedColumn, as: Int32.self)
        case .uint32:
          try testFixedWidth(
            actual: arrowArray, expected: expectedColumn, as: UInt32.self)
        case .int64:
          try testFixedWidth(
            actual: arrowArray, expected: expectedColumn, as: Int64.self)
        case .uint64:
          try testFixedWidth(
            actual: arrowArray, expected: expectedColumn, as: UInt64.self)
        case .float32:
          try testFixedWidth(
            actual: arrowArray, expected: expectedColumn, as: Float.self)
        case .float64:
          try testFixedWidth(
            actual: arrowArray, expected: expectedColumn, as: Double.self)
        case .binary:
          try testVariableLength(
            actual: arrowArray, expected: expectedColumn, type: arrowField.type)
        case .utf8:
          try testVariableLength(
            actual: arrowArray, expected: expectedColumn, type: arrowField.type)
        case .list(_):
          try validateListArray(actual: arrowArray, expected: expectedColumn)
          break
        case .fixedSizeList(_, let listSize):
          try validateFixedWidthListArray(
            actual: arrowArray,
            expected: expectedColumn,
            listSize: listSize
          )
          break
        //        case .strct(let fields):

        default:
          //          throw ArrowError.invalid(
          print(
            "TODO: Implement test for arrow field type: \(arrowField.type)")
        }
      }
    }
  }

  func testFixedWidthBinary(
    actual: ArrowArrayOfData,
    expected: ArrowGold.Column,
  ) throws {
    guard let validity = expected.validity, let dataValues = expected.data
    else {
      throw ArrowError.invalid("Test column is incomplete.")
    }
    for (i, isNull) in validity.enumerated() {
      guard case .string(let hex) = dataValues[i] else {
        throw ArrowError.invalid("Data values are not all strings.")
      }
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

  func testBoolean(
    actual: AnyArrowArrayProtocol,
    expected: ArrowGold.Column
  ) throws {
    guard let expectedValidity = expected.validity,
      let expectedValues = expected.data
    else {
      throw ArrowError.invalid("Test column is incomplete.")
    }
    guard let array = actual as? ArrowArrayBoolean,
      array.length == expectedValidity.count
    else {
      Issue.record("Array type mismatch")
      return
    }
    for (i, isNull) in expectedValidity.enumerated() {
      guard case .bool(let expectedValue) = expectedValues[i] else {
        throw ArrowError.invalid("Expected boolean value")
      }
      if isNull == 0 {
        #expect(array[i] == nil)
      } else {
        #expect(array[i] == expectedValue)
      }
    }
  }

  func testFixedWidth<T>(
    actual: AnyArrowArrayProtocol,
    expected: ArrowGold.Column,
    as type: T.Type
  ) throws where T: BinaryInteger & LosslessStringConvertible {
    guard let expectedValidity = expected.validity,
      let expectedValues = expected.data
    else {
      throw ArrowError.invalid("Test column is incomplete.")
    }
    guard let array = actual as? any ArrowArrayProtocol,
      array.length == expectedValidity.count
    else {
      Issue.record("Array type mismatch")
      return
    }
    for (i, isNull) in expectedValidity.enumerated() {
      let expected: T
      if case .int(let intVal) = expectedValues[i] {
        expected = try T(throwingOnOverflow: intVal)
      } else if case .string(let strVal) = expectedValues[i],
        let parsed = T(strVal)
      {
        expected = parsed
      } else {
        throw ArrowError.invalid("Expected integer value or numeric string")
      }

      if isNull == 0 {
        #expect(array[i] == nil)
      } else {
        #expect(array[i] as? T == expected)
      }
    }
  }

  func testFixedWidth<T>(
    actual: AnyArrowArrayProtocol,
    expected: ArrowGold.Column,
    as type: T.Type
  ) throws where T: BinaryFloatingPoint & LosslessStringConvertible {
    guard let expectedValidity = expected.validity,
      let expectedValues = expected.data
    else {
      throw ArrowError.invalid("Test column is incomplete.")
    }
    guard let array = actual as? any ArrowArrayProtocol,
      array.length == expectedValidity.count
    else {
      Issue.record("Array type mismatch")
      return
    }
    for (i, isNull) in expectedValidity.enumerated() {
      let expected: T
      if case .double(let doubleVal) = expectedValues[i] {
        expected = T(doubleVal)
      } else if case .string(let strVal) = expectedValues[i],
        let parsed = T(strVal)
      {
        expected = parsed
      } else {
        throw ArrowError.invalid("Expected float value or numeric string")
      }
      if isNull == 0 {
        #expect(array[i] == nil)
      } else {
        #expect(array[i] as? T == expected)
      }
    }
  }

  func validateFixedWidthListArray(
    actual: AnyArrowArrayProtocol,
    expected: ArrowGold.Column,
    listSize: Int32
  ) throws {

    guard let expectedValidity = expected.validity
    else {
      throw ArrowError.invalid("Test column is incomplete.")
    }
    guard let listArray = actual as? ArrowFixedSizeListArray
    else {
      Issue.record("Unexpected array type: \(type(of: actual))")
      return
    }

    for (i, isNull) in expectedValidity.enumerated() {
      if isNull == 0 {
        #expect(listArray[i] == nil)
      } else {
        guard let actualChildSlice = listArray[i] else {
          Issue.record("Expected non-null list at index \(i)")
          continue
        }
        #expect(actualChildSlice.length == listSize)
      }
    }
  }

  func validateListArray(
    actual: AnyArrowArrayProtocol,
    expected: ArrowGold.Column
  ) throws {
    guard let expectedValidity = expected.validity,
      let expectedOffsets = expected.offset
    else {
      throw ArrowError.invalid("Test column is incomplete.")
    }

    // Validate the offsets buffer
    actual.buffers[1].withUnsafeBytes { ptr in
      let offsets = ptr.bindMemory(to: Int32.self)
      #expect(offsets.count == expectedOffsets.count)
      for (i, expectedOffset) in expectedOffsets.enumerated() {
        let actualOffset = offsets[i]
        #expect(actualOffset == expectedOffset)
      }
    }

    // TODO: Need a simpler type signature at call site.
    guard let listArray = actual as? ArrowListArray<FixedWidthBufferIPC<Int32>>
    else {
      Issue.record("Unexpected array type: \(type(of: actual))")
      return
    }

    guard let child = expected.children?.first else {
      throw ArrowError.invalid("List array missing child column")
    }

    // Validate each list entry
    for (i, isNull) in expectedValidity.enumerated() {
      if isNull == 0 {
        #expect(listArray[i] == nil)
      } else {
        guard let actualChildSlice = listArray[i] else {
          Issue.record("Expected non-null list at index \(i)")
          continue
        }

        // Get expected range from offsets
        let childStartOffset = Int(expectedOffsets[i])
        let childEndOffset = Int(expectedOffsets[i + 1])
        let expectedLength = childEndOffset - childStartOffset

        #expect(actualChildSlice.length == expectedLength)

        // Validate each element in this list
        for j in 0..<actualChildSlice.length {
          let expectedDataIndex = childStartOffset + j

          // Check validity if present
          if let childValidity = child.validity {
            if childValidity[expectedDataIndex] == 0 {
              // Expected null
              // Need to check actualChildSlice[j] is null
              // This depends on your array type - might need type-specific handling
              continue
            }
          }

          // Validate the actual value based on child type
          // This is where you'd dispatch based on child column type
          guard let childData = child.data else {
            throw ArrowError.invalid("Child column missing DATA")
          }

          // TODO:  Type-specific validation
          guard case .int(let expectedValue) = childData[expectedDataIndex]
          else {
            throw ArrowError.invalid("Unexpected child data type")
          }
        }
      }
    }
  }

  func testVariableLength(
    actual: AnyArrowArrayProtocol,
    expected: ArrowGold.Column,
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
        guard case .string(let hex) = expectedValues[i] else {
          throw ArrowError.invalid("Data values are not all strings.")
        }
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
        guard case .string(let utf8) = expectedValues[i] else {
          throw ArrowError.invalid("Data values are not all strings.")
        }
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
