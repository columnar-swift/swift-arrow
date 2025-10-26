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
  field: FlatField
) throws(ArrowError) -> ArrowField {
  let type = field.typeType
  var arrowType = ArrowType(ArrowType.arrowUnknown)
  switch type {
  case .int:
    guard let intType = field.type(type: FlatInt.self) else {
      throw .invalid("Invalid FlatBuffer: \(field)")
    }
    let bitWidth = intType.bitWidth
    if bitWidth == 8 {
      arrowType = ArrowType(intType.isSigned ? ArrowType.arrowInt8 : ArrowType.arrowUInt8)
    } else if bitWidth == 16 {
      arrowType = ArrowType(intType.isSigned ? ArrowType.arrowInt16 : ArrowType.arrowUInt16)
    } else if bitWidth == 32 {
      arrowType = ArrowType(intType.isSigned ? ArrowType.arrowInt32 : ArrowType.arrowUInt32)
    } else if bitWidth == 64 {
      arrowType = ArrowType(intType.isSigned ? ArrowType.arrowInt64 : ArrowType.arrowUInt64)
    }
  case .bool:
    arrowType = ArrowType(ArrowType.arrowBool)
  case .floatingpoint:
    guard let floatType = field.type(type: FloatingPoint.self) else {
      throw .invalid("Invalid FlatBuffer: \(field)")
    }
    if floatType.precision == .single {
      arrowType = ArrowType(ArrowType.arrowFloat)
    } else if floatType.precision == .double {
      arrowType = ArrowType(ArrowType.arrowDouble)
    }
  case .utf8:
    arrowType = ArrowType(ArrowType.arrowString)
  case .binary:
    arrowType = ArrowType(ArrowType.arrowBinary)
  case .date:
    guard let dateType = field.type(type: FlatDate.self) else {
      throw .invalid("Invalid FlatBuffer: \(field)")
    }
    if dateType.unit == .day {
      arrowType = ArrowType(ArrowType.arrowDate32)
    } else {
      arrowType = ArrowType(ArrowType.arrowDate64)
    }
  case .time:
    guard let timeType = field.type(type: FlatTime.self) else {
      throw .invalid("Invalid FlatBuffer: \(field)")
    }
    if timeType.unit == .second || timeType.unit == .millisecond {
      let arrowUnit: ArrowTime32Unit = timeType.unit == .second ? .seconds : .milliseconds
      arrowType = ArrowTypeTime32(arrowUnit)
    } else {
      let arrowUnit: ArrowTime64Unit = timeType.unit == .microsecond ? .microseconds : .nanoseconds
      arrowType = ArrowTypeTime64(arrowUnit)
    }
  case .timestamp:
    guard let timestampType = field.type(type: FlatTimestamp.self) else {
      throw .invalid("Invalid FlatBuffer: \(field)")
    }
    let arrowUnit: ArrowTimestampUnit
    switch timestampType.unit {
    case .second:
      arrowUnit = .seconds
    case .millisecond:
      arrowUnit = .milliseconds
    case .microsecond:
      arrowUnit = .microseconds
    case .nanosecond:
      arrowUnit = .nanoseconds
    }
    let timezone = timestampType.timezone
    arrowType = ArrowTypeTimestamp(arrowUnit, timezone: timezone?.isEmpty == true ? nil : timezone)
  case .struct_:
    var children = [ArrowField]()
    for index in 0..<field.childrenCount {
      guard let childField = field.children(at: index) else {
        throw .invalid("Missing childe at index: \(index) for field: \(field)")
      }
      children.append(try fromProto(field: childField))
    }
    arrowType = ArrowTypeStruct(ArrowType.arrowStruct, fields: children)
  case .list:
    guard field.childrenCount == 1, let childField = field.children(at: 0) else {
      arrowType = ArrowType(ArrowType.arrowUnknown)
      break
    }
    let childArrowField = try fromProto(field: childField)
    arrowType = ArrowTypeList(childArrowField.type)
  default:
    arrowType = ArrowType(ArrowType.arrowUnknown)
  }
  guard let fieldName = field.name else {
    throw .invalid("Invalid FlatBuffer: \(field)")
  }
  return ArrowField(fieldName, type: arrowType, isNullable: field.nullable)
}
