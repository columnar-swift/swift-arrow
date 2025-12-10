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
import FlatBuffers

extension ArrowWriter {

  func write(
    field: ArrowField,
    to fbb: inout FlatBufferBuilder,
  ) throws(ArrowError) -> Offset {
    var fieldsOffset: Offset?
    if case .strct(let fields) = field.type {
      var offsets: [Offset] = []
      for field in fields {
        let offset = try write(field: field, to: &fbb)
        offsets.append(offset)
      }
      fieldsOffset = fbb.createVector(ofOffsets: offsets)
    }
    let nameOffset = fbb.create(string: field.name)
    let fieldTypeOffset = try append(arrowType: field.type, to: &fbb)
    let startOffset = FField.startField(&fbb)
    FField.add(name: nameOffset, &fbb)
    FField.add(nullable: field.isNullable, &fbb)
    if let childrenOffset = fieldsOffset {
      FField.addVectorOf(children: childrenOffset, &fbb)
    }
    let typeType = try field.type.fType()
    FField.add(typeType: typeType, &fbb)
    FField.add(type: fieldTypeOffset, &fbb)
    return FField.endField(&fbb, start: startOffset)
  }

  func append(
    arrowType: ArrowType,
    to fbb: inout FlatBufferBuilder,
  ) throws(ArrowError) -> Offset {
    switch arrowType {
    case .int8, .uint8:
      return FInt.createInt(&fbb, bitWidth: 8, isSigned: arrowType == .int8)
    case .int16, .uint16:
      return FInt.createInt(&fbb, bitWidth: 16, isSigned: arrowType == .int16)
    case .int32, .uint32:
      return FInt.createInt(&fbb, bitWidth: 32, isSigned: arrowType == .int32)
    case .int64, .uint64:
      return FInt.createInt(&fbb, bitWidth: 64, isSigned: arrowType == .int64)
    case .float16:
      return FFloatingPoint.createFloatingPoint(&fbb, precision: .half)
    case .float32:
      return FFloatingPoint.createFloatingPoint(&fbb, precision: .single)
    case .float64:
      return FFloatingPoint.createFloatingPoint(&fbb, precision: .double)
    case .utf8:
      return FUtf8.endUtf8(&fbb, start: FUtf8.startUtf8(&fbb))
    case .binary:
      return FBinary.endBinary(&fbb, start: FBinary.startBinary(&fbb))
    case .boolean:
      return FBool.endBool(&fbb, start: FBool.startBool(&fbb))
    case .date32:
      let startOffset = FDate.startDate(&fbb)
      FDate.add(unit: .day, &fbb)
      return FDate.endDate(&fbb, start: startOffset)
    case .date64:
      let startOffset = FDate.startDate(&fbb)
      FDate.add(unit: .millisecond, &fbb)
      return FDate.endDate(&fbb, start: startOffset)
    case .time32(let unit):
      let startOffset = FTime.startTime(&fbb)
      FTime.add(unit: unit == .second ? .second : .millisecond, &fbb)
      return FTime.endTime(&fbb, start: startOffset)
    case .time64(let unit):
      let startOffset = FTime.startTime(&fbb)
      FTime.add(unit: unit == .microsecond ? .microsecond : .nanosecond, &fbb)
      return FTime.endTime(&fbb, start: startOffset)
    case .timestamp(let unit, let timezone):
      let startOffset = FTimestamp.startTimestamp(&fbb)
      let fbUnit: FTimeUnit
      switch unit {
      case .second:
        fbUnit = .second
      case .millisecond:
        fbUnit = .millisecond
      case .microsecond:
        fbUnit = .microsecond
      case .nanosecond:
        fbUnit = .nanosecond
      }
      FTimestamp.add(unit: fbUnit, &fbb)
      if let timezone {
        let timezoneOffset = fbb.create(string: timezone)
        FTimestamp.add(timezone: timezoneOffset, &fbb)
      }
      return FTimestamp.endTimestamp(&fbb, start: startOffset)
    case .strct(_):
      let startOffset = FStruct.startStruct_(&fbb)
      return FStruct.endStruct_(&fbb, start: startOffset)
    default:
      throw .init(.unknownType(
        "Unable to add flatbuf type for Arrow type: \(arrowType)"))
    }
  }
}
