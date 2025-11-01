// Copyright 2025 The Apache Software Foundation
// Copyright 2025 The Columnar-Swift Contributors
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

func makeBuffer(
  _ buffer: Buffer,
  fileData: Data,
  length: UInt,
  messageOffset: Int64
) -> ArrowBuffer {
  let startOffset = messageOffset + buffer.offset
  let endOffset = startOffset + buffer.length
  let bufferData = [UInt8](fileData[startOffset..<endOffset])
  return ArrowBuffer.createBuffer(bufferData, length: length)
}

func isFixedPrimitive(_ type: FlatType) -> Bool {
  switch type {
  case .int, .bool, .floatingpoint, .date, .time, .timestamp:
    return true
  default:
    return false
  }
}

func findArrowType(_ field: FlatField) throws(ArrowError) -> ArrowType {
  let type = field.typeType
  switch type {
  case .int:
    guard let intType = field.type(type: FlatInt.self) else {
      throw .invalid("Could not get integer type from \(field)")
    }
    let bitWidth = intType.bitWidth
    if bitWidth == 8 {
      if intType.isSigned {
        return .int8
      } else {
        return .uint8
      }
    }
    if bitWidth == 16 {
      return intType.isSigned ? .int16 : .uint16
    }
    if bitWidth == 32 {
      return intType.isSigned ? .int32 : .uint32
    }
    if bitWidth == 64 {
      return intType.isSigned ? .int64 : .uint64
    }
    throw .invalid("Unhandled integer bit width: \(bitWidth)")
  case .bool:
    return .boolean
  case .floatingpoint:
    guard let floatType = field.type(type: FloatingPoint.self) else {
      throw .invalid("Could not get floating point type from field")
    }
    switch floatType.precision {
    case .half:
      return .float16
    case .single:
      return .float32
    case .double:
      return .float64
    }
  case .utf8:
    return .utf8
  case .binary:
    return .binary
  case .date:
    guard let dateType = field.type(type: FlatDate.self) else {
      throw .invalid("Could not get date type from field")
    }
    if dateType.unit == .day {
      return .date32
    }
    return .date64
  case .time:
    guard let timeType = field.type(type: FlatTime.self) else {
      throw .invalid("Could not get time type from field")
    }
    if timeType.unit == .second || timeType.unit == .millisecond {
      return .time32(
        timeType.unit == .second ? .second : .millisecond
      )
    }
    return .time64(
      timeType.unit == .microsecond ? .microsecond : .nanosecond
    )
  case .timestamp:
    guard let timestampType = field.type(type: FlatTimestamp.self) else {
      throw .invalid("Could not get timestamp type from field")
    }
    let arrowUnit: TimeUnit
    switch timestampType.unit {
    case .second:
      arrowUnit = .second
    case .millisecond:
      arrowUnit = .millisecond
    case .microsecond:
      arrowUnit = .microsecond
    case .nanosecond:
      arrowUnit = .nanosecond
    }
    let timezone = timestampType.timezone
    return .timestamp(arrowUnit, timezone)
  case .struct_:
    guard field.type(type: FlatStruct.self) != nil else {
      throw .invalid("Could not get struct type from field")
    }
    var fields: [ArrowField] = []
    for index in 0..<field.childrenCount {
      guard let childField = field.children(at: index) else {
        throw .invalid("Could not get child at index: \(index) ofrom struct")
      }
      let childType = try findArrowType(childField)
      guard let name = childField.name else {
        throw .invalid("Could not get name of child field")
      }
      fields.append(
        ArrowField(
          name: name,
          dataType: childType,
          isNullable: childField.nullable
        )
      )
    }
    return .strct(fields)
  case .list:
    guard field.childrenCount == 1, let childField = field.children(at: 0)
    else {
      throw .invalid("Expected list field to have exactly one child")
    }
    let childType = try findArrowType(childField)
    guard let name = childField.name else {
      throw .invalid("Could not get name of child field")
    }
    let arrowField = ArrowField(
      name: name,
      dataType: childType,
      isNullable: childField.nullable
    )
    return .list(arrowField)
  default:
    throw .invalid("Unhandled field type: \(field.typeType)")
  }
}

func validateBufferIndex(_ recordBatch: FlatRecordBatch, index: Int32) throws {
  if index >= recordBatch.buffersCount {
    throw ArrowError.outOfBounds(index: Int64(index))
  }
}
