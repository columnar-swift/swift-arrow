// Copyright 2025 The Apache Software Foundation
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

extension ArrowType {

  /// Looks up the `ArrowType` equivalent for a FlatBuffers `Field`.
  /// - Parameter field: The FlatBuffers `Field`.
  /// - Returns: The `ArrowType`
  /// - Throws: An `ArrowError` if lookup fails.
  static func type(for field: FField) throws(ArrowError) -> Self {
    let type = field.typeType
    switch type {
    case .int:
      guard let intType = field.type(type: FInt.self) else {
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
      guard let floatType = field.type(type: FFloatingPoint.self) else {
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
      guard let dateType = field.type(type: FDate.self) else {
        throw .invalid("Could not get date type from field")
      }
      if dateType.unit == .day {
        return .date32
      }
      return .date64
    case .time:
      guard let timeType = field.type(type: FTime.self) else {
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
      guard let timestampType = field.type(type: FTimestamp.self) else {
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
      guard field.type(type: FStruct.self) != nil else {
        throw .invalid("Could not get struct type from field")
      }
      var fields: [ArrowField] = []
      for index in 0..<field.childrenCount {
        guard let childField = field.children(at: index) else {
          throw .invalid("Could not get child at index: \(index) ofrom struct")
        }
        let childType = try self.type(for: childField)
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
      let childType = try self.type(for: childField)
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

}
