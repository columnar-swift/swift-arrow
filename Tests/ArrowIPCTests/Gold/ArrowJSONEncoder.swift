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

/// Encode an array to the gold testing JSON format.
/// - Parameters:
///   - array: The array to encode.
///   - field: The field associated with the array.
/// - Throws: An `ArrowError` if encoding fails.
/// - Returns: The column exactly as the test format expects it.
/// Note the junk values present in the test data are not replicated here therefore these need to be
/// removed from test data before comparison happens.
func encodeColumn(
  array: AnyArrowArrayProtocol,
  field: ArrowField
) throws(ArrowError) -> ArrowGold.Column {
  guard let array = array as? (any ArrowArrayProtocol) else {
    throw .invalid("Expected ArrowArray, got \(type(of: array))")
  }
  // Validity is always present in the gold files.
  let validity: [Int] = (0..<array.length).map { i in
    array[i] == nil ? 0 : 1
  }
  // Offsets are taken directly from the buffers. Generating them from the
  // public API would mean replicating edge cases here.
  let offsets: [Int]? =
    switch field.type {
    case .binary, .utf8, .list(_), .map(_, _):
      array.buffers[1].withUnsafeBytes { ptr in
        let offsets = ptr.bindMemory(to: Int32.self)
        return Array(offsets).map(Int.init)
      }
    case .largeBinary, .largeUtf8, .largeList(_):
      array.buffers[1].withUnsafeBytes { ptr in
        let offsets = ptr.bindMemory(to: Int64.self)
        return Array(offsets).map(Int.init)
      }
    default: nil
    }
  // Data are retrieved via the public interface to test the array API.
  var data: [DataValue]? = []
  var children: [ArrowGold.Column]? = nil
  if array.length > 0 {
    switch field.type {
    case .list(let listField), .map(let listField, _):
      guard let listArray = array as? ListArrayProtocol else {
        throw ArrowError.invalid("Expected list array")
      }
      let childColumn = try encodeColumn(
        array: listArray.values, field: listField)
      children = [childColumn]
      // List arrays point to child arrays therefore have nil data buffers.
      data = nil
    case .fixedSizeList(let listField, _):
      guard let listArray = array as? ListArrayProtocol else {
        throw ArrowError.invalid("Expected list array")
      }
      let childColumn = try encodeColumn(
        array: listArray.values, field: listField)
      children = [childColumn]
      data = nil
    case .strct(let arrowFields):
      guard let structArray = array as? ArrowStructArray else {
        throw ArrowError.invalid("Expected list array")
      }
      children = []
      for (arrowField, (_, array)) in zip(arrowFields, structArray.fields) {
        let childColumn = try encodeColumn(
          array: array, field: arrowField)
        children?.append(childColumn)
        data = nil
      }
      children = try arrowFields.enumerated().map {
        index, arrowField throws(ArrowError) in
        try encodeColumn(array: structArray.fields[index].1, field: arrowField)
      }
      data = nil
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
    case .binary:
      try extractBinaryData(from: array, into: &data)
    case .fixedSizeBinary(_):
      try extractBinaryData(from: array, into: &data)
    case .utf8:
      try extractUtf8Data(from: array, into: &data)
    default:
      throw .invalid("Encoder did not handle a field type: \(field.type)")
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
) throws(ArrowError) -> [DataValue] {
  guard let typedArray = array as? ArrowArrayNumeric<T> else {
    throw .invalid("Expected \(T.self) array, got \(type(of: array))")
  }
  do {
    return try (0..<typedArray.length).map { i in
      guard let value = typedArray[i] else { return .null }

      // 64 bit types are encoded as strings.
      if expectedType.bitWidth == 64 {
        return .string("\(value)")
      } else {
        return .int(try Int(throwingOnOverflow: value))
      }
    }
  } catch {
    throw .invalid("Failed to extract Int data: \(error)")
  }
}

func extractFloatData<T: BinaryFloatingPoint & BitwiseCopyable>(
  from array: AnyArrowArrayProtocol,
  expectedType: T.Type
) throws(ArrowError) -> [DataValue] {
  guard let typedArray = array as? ArrowArrayNumeric<T> else {
    throw ArrowError.invalid("Expected \(T.self) array, got \(type(of: array))")
  }
  let encoder = JSONEncoder()
  let decoder = JSONDecoder()
  do {
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
  } catch {
    throw .ioError("Failed to round-trip float to/from JSON")
  }
}

func extractBoolData(
  from array: AnyArrowArrayProtocol
) throws(ArrowError) -> [DataValue] {
  guard let typedArray = array as? ArrowArrayBoolean else {
    throw .invalid("Expected boolean array, got \(type(of: array))")
  }
  return (0..<typedArray.length).map { i in
    guard let value = typedArray[i] else { return .null }
    return .bool(value)
  }
}

func extractBinaryData(
  from array: AnyArrowArrayProtocol,
  into dataValues: inout [DataValue]?
) throws(ArrowError) {
  guard let binaryArray = array as? any BinaryArrayProtocol else {
    throw .invalid("Expected binary array")
  }
  dataValues = (0..<binaryArray.length).map { i in
    guard let value = binaryArray[i] else {
      return .null
    }
    let hexString = value.map { String(format: "%02X", $0) }.joined()
    return .string(hexString)
  }
}

func extractUtf8Data(
  from array: AnyArrowArrayProtocol,
  into dataValues: inout [DataValue]?
) throws(ArrowError) {
  guard let stringArray = array as? any Utf8ArrayProtocol else {
    throw .invalid("Expected UTF-8 array")
  }
  dataValues = (0..<stringArray.length).map { i in
    guard let value = stringArray[i] else {
      return .null
    }
    return .string(value)
  }
}
