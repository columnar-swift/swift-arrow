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

@testable import Arrow

func encodeColumn(
  array: AnyArrowArrayProtocol,
  field: ArrowField
) throws -> ArrowGold.Column {

  guard let array = array as? (any ArrowArrayProtocol) else {
    throw ArrowError.invalid("Expected ArrowArray, got \(type(of: array))")
  }

  var validity: [Int] = []

  for i in 0..<array.length {
    let value: Any? = array[i]
    validity.append(value == nil ? 0 : 1)
  }

  var offsets: [Int]? = nil
  var data: [DataValue]? = nil
  var children: [ArrowGold.Column]? = nil

  switch field.type {
  // Test the actual array interface
  case .list(let listField):
    guard let listArray = array as? ArrowArrayOfList else {
      throw ArrowError.invalid("Expected list array")
    }
    // Build offsets by using the array interface
    var computedOffsets: [Int] = [0]
    var currentOffset = 0

    for i in 0..<listArray.length {
      if let list = listArray[i] {
        currentOffset += list.length
      }
      // Null lists don't advance the offset
      computedOffsets.append(currentOffset)
    }
    offsets = computedOffsets

    // Recursively encode all list values
    let childColumn = try encodeColumn(
      array: listArray.values, field: listField)
    children = [childColumn]

  case .int8:
    data = try extractIntData(from: array, expectedType: Int8.self)
  case .uint8:
    data = try extractIntData(from: array, expectedType: UInt8.self)

  //    for i in 0..<array.length {
  //      let value: Any? = array[i]
  //      let dataValue: DataValue = .int(value)
  //      data?.append(dataValue)
  //    }

  default:
    print("Unhandled type: \(field.type)")
  }

  return .init(
    name: field.name,
    count: array.length,
    validity: validity,
    offset: offsets,
    data: data,
    children: children
  )
}

func extractIntData<T: FixedWidthInteger & BitwiseCopyable>(
  from array: AnyArrowArrayProtocol,
  expectedType: T.Type
) throws -> [DataValue] {
  guard let typedArray = array as? ArrowArrayNumeric<T> else {
    throw ArrowError.invalid("Expected \(T.self) array, got \(type(of: array))")
  }

  return try (0..<typedArray.length).map { i in
    guard let value = typedArray[i] else { return .null }
    return .int(try Int(throwingOnOverflow: value))
  }
}
