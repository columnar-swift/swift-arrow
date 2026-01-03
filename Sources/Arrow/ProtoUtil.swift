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

import Foundation

func fromProto(
  field: FField
) throws(ArrowError) -> ArrowField {
  let type = field.typeType
  var arrowType: ArrowType?
  switch type {
  case .int:
    guard let intType = field.type(type: FInt.self) else {
      throw .init(.invalid("Invalid FlatBuffer: \(field)"))
    }
    let bitWidth = intType.bitWidth
    if bitWidth == 8 {
      arrowType = intType.isSigned ? .int8 : .uint8
    } else if bitWidth == 16 {
      arrowType = intType.isSigned ? .int16 : .uint16
    } else if bitWidth == 32 {
      arrowType = intType.isSigned ? .int32 : .uint32
    } else if bitWidth == 64 {
      arrowType = intType.isSigned ? .int64 : .uint64
    }
  case .bool:
    arrowType = .boolean
  case .floatingpoint:
    guard let floatType = field.type(type: FFloatingPoint.self) else {
      throw .init(.invalid("Invalid FlatBuffer: \(field)"))
    }
    switch floatType.precision {
    case .half:
      arrowType = .float16
    case .single:
      arrowType = .float32
    case .double:
      arrowType = .float64
    }
  case .utf8:
    arrowType = .utf8
  case .binary:
    arrowType = .binary
  case .date:
    guard let dateType = field.type(type: FDate.self) else {
      throw .init(.invalid("Invalid FlatBuffer: \(field)"))
    }
    if dateType.unit == .day {
      arrowType = .date32
    } else {
      arrowType = .date64
    }
  case .time:
    guard let timeType = field.type(type: FTime.self) else {
      throw .init(.invalid("Invalid FlatBuffer: \(field)"))
    }
    if timeType.unit == .second || timeType.unit == .millisecond {
      let arrowUnit: TimeUnit =
        timeType.unit == .second ? .second : .millisecond
      arrowType = .time32(arrowUnit)
    } else {
      let arrowUnit: TimeUnit =
        timeType.unit == .microsecond ? .microsecond : .nanosecond
      arrowType = .time64(arrowUnit)
    }
  case .timestamp:
    guard let timestampType = field.type(type: FTimestamp.self) else {
      throw .init(.invalid("Invalid FlatBuffer: \(field)"))
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
    arrowType = .timestamp(arrowUnit, timestampType.timezone)
  case .struct_:
    var children: [ArrowField] = []
    for index in 0..<field.childrenCount {
      guard let childField = field.children(at: index) else {
        throw .init(
          .invalid("Missing childe at index: \(index) for field: \(field)"))
      }
      children.append(try fromProto(field: childField))
    }
    arrowType = .strct(children)
  case .list:
    guard field.childrenCount == 1, let childField = field.children(at: 0)
    else {
      throw .init(.invalid("Expected a single child for list field: \(field)"))
    }
    let childArrowField = try fromProto(field: childField)
    arrowType = .list(childArrowField)
  default:
    throw .init(.invalid("Unsupported FlatBuffer field type: \(field)"))
  }
  guard let fieldName = field.name else {
    throw .init(.invalid("Invalid FlatBuffer: \(field)"))
  }
  guard let arrowType else {
    throw .init(.invalid("Unsupported FlatBuffer field type: \(field)"))
  }
  return ArrowField(
    name: fieldName,
    dataType: arrowType,
    isNullable: field.nullable
  )
}
