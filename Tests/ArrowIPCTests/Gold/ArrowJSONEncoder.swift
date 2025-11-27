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

  if array.length > 0 {

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

    case .boolean:
      data = try extractBoolData(from: array)
    case .int8:
      data = try extractIntData(from: array, expectedType: Int8.self)
    case .int16:
      data = try extractIntData(from: array, expectedType: Int16.self)
    case .int32:
      data = try extractIntData(from: array, expectedType: Int32.self)
    case .int64:
      data = try extractIntData(from: array, expectedType: Int64.self)
    case .uint8:
      data = try extractIntData(from: array, expectedType: UInt8.self)
    case .uint16:
      data = try extractIntData(from: array, expectedType: UInt16.self)
    case .uint32:
      data = try extractIntData(from: array, expectedType: UInt32.self)
    case .uint64:
      data = try extractIntData(from: array, expectedType: UInt64.self)
    case .float16:
      data = try extractFloatData(from: array, expectedType: Float16.self)
    case .float32:
      data = try extractFloatData(from: array, expectedType: Float32.self)
    case .float64:
      data = try extractFloatData(from: array, expectedType: Float64.self)
    default:
      print("Unhandled type: \(field.type)")
    }
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

    // 64 bit types are encoded as strings.
    if expectedType.bitWidth == 64 {
      return .string("\(value)")
    } else {
      return .int(try Int(throwingOnOverflow: value))
    }
  }
}

func extractFloatData<T: BinaryFloatingPoint & BitwiseCopyable>(
  from array: AnyArrowArrayProtocol,
  expectedType: T.Type
) throws -> [DataValue] {
  guard let typedArray = array as? ArrowArrayNumeric<T> else {
    throw ArrowError.invalid("Expected \(T.self) array, got \(type(of: array))")
  }

  let encoder = JSONEncoder()
  let decoder = JSONDecoder()

  return try (0..<typedArray.length).map { i in
    guard let value = typedArray[i] else { return .null }

    // Round-trip through JSON to match input format exactly
    if let v = value as? Float {
      let data = try encoder.encode(v)
      let jsonNumber = try decoder.decode(Float.self, from: data)
      return .string(String(jsonNumber))
    } else if let v = value as? Double {
      let data = try encoder.encode(v)
      let jsonNumber = try decoder.decode(Double.self, from: data)
      return .string(String(jsonNumber))
    } else if let v = value as? Float16 {
      let asFloat = Float(v)
      let data = try encoder.encode(asFloat)
      let jsonNumber = try decoder.decode(Float.self, from: data)
      return .string(String(jsonNumber))
    } else {
      throw ArrowError.invalid("Expected float type")
    }
  }
}

func extractBoolData(from array: AnyArrowArrayProtocol) throws -> [DataValue] {
  guard let typedArray = array as? ArrowArrayBoolean else {
    throw ArrowError.invalid("Expected boolean array, got \(type(of: array))")
  }
  return (0..<typedArray.length).map { i in
    guard let value = typedArray[i] else { return .null }
    return .bool(value)
  }
}

//func extractBinaryData(
//  from array: AnyArrowArrayProtocol
//) throws -> [DataValue] {
//  guard let binaryArray = array as? ArrowArrayBinary else {
//    throw ArrowError.invalid("Expected binary array")
//  }
//
//  return (0..<binaryArray.length).map { i in
//    guard let data = binaryArray[i] else { return .null }
//    // Hex encode the bytes
//    let hexString = data.map { String(format: "%02x", $0) }.joined()
//    return .string(hexString)
//  }
//}
