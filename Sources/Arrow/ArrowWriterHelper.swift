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

import FlatBuffers
import Foundation

extension Data {
  func hexEncodedString() -> String {
    map { String(format: "%02hhx", $0) }.joined()
  }
}

func toFBTypeEnum(_ arrowType: ArrowType) -> Result<FType, ArrowError> {
  let typeId = arrowType
  switch typeId {
  case .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64:
    return .success(FType.int)
  case .float16, .float32, .float64:
    return .success(FType.floatingpoint)
  case .utf8:
    return .success(FType.utf8)
  case .binary:
    return .success(FType.binary)
  case .boolean:
    return .success(FType.bool)
  case .date32, .date64:
    return .success(FType.date)
  case .time32, .time64:
    return .success(FType.time)
  case .timestamp:
    return .success(FType.timestamp)
  case .strct:
    return .success(FType.struct_)
  default:
    return .failure(
      .init(
        .unknownType("Unable to find flatbuf type for Arrow type: \(typeId)")
      )
    )
  }
}

func toFBType(
  _ fbb: inout FlatBufferBuilder,
  arrowType: ArrowType
) -> Result<Offset, ArrowError> {
  //  let infoType = arrowType.info
  switch arrowType {
  case .int8, .uint8:
    return .success(
      FInt.createInt(&fbb, bitWidth: 8, isSigned: arrowType == .int8))
  case .int16, .uint16:
    return .success(
      FInt.createInt(&fbb, bitWidth: 16, isSigned: arrowType == .int16))
  case .int32, .uint32:
    return .success(
      FInt.createInt(&fbb, bitWidth: 32, isSigned: arrowType == .int32))
  case .int64, .uint64:
    return .success(
      FInt.createInt(&fbb, bitWidth: 64, isSigned: arrowType == .int64))
  case .float16:
    return .success(FFloatingPoint.createFloatingPoint(&fbb, precision: .half))
  case .float32:
    return .success(
      FFloatingPoint.createFloatingPoint(&fbb, precision: .single))
  case .float64:
    return .success(
      FFloatingPoint.createFloatingPoint(&fbb, precision: .double))
  case .utf8:
    return .success(FUtf8.endUtf8(&fbb, start: FUtf8.startUtf8(&fbb)))
  case .binary:
    return .success(FBinary.endBinary(&fbb, start: FBinary.startBinary(&fbb)))
  case .boolean:
    return .success(FBool.endBool(&fbb, start: FBool.startBool(&fbb)))
  case .date32:
    let startOffset = FDate.startDate(&fbb)
    FDate.add(unit: .day, &fbb)
    return .success(FDate.endDate(&fbb, start: startOffset))
  case .date64:
    let startOffset = FDate.startDate(&fbb)
    FDate.add(unit: .millisecond, &fbb)
    return .success(FDate.endDate(&fbb, start: startOffset))
  case .time32(let unit):
    let startOffset = FTime.startTime(&fbb)
    FTime.add(unit: unit == .second ? .second : .millisecond, &fbb)
    return .success(FTime.endTime(&fbb, start: startOffset))
  case .time64(let unit):
    let startOffset = FTime.startTime(&fbb)
    FTime.add(unit: unit == .microsecond ? .microsecond : .nanosecond, &fbb)
    return .success(FTime.endTime(&fbb, start: startOffset))
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
    return .success(FTimestamp.endTimestamp(&fbb, start: startOffset))
  case .strct(_):
    let startOffset = FStruct.startStruct_(&fbb)
    return .success(FStruct.endStruct_(&fbb, start: startOffset))
  default:
    return .failure(
      .init(
        .unknownType("Unable to add flatbuf type for Arrow type: \(arrowType)"))
    )
  }
}

func addPadForAlignment(_ data: inout Data, alignment: Int = 8) {
  let padding = data.count % Int(alignment)
  if padding > 0 {
    data.append(Data([UInt8](repeating: 0, count: alignment - padding)))
  }
}

//func addPadForAlignment(_ writer: inout DataWriter, alignment: Int = 8) {
//  let padding = writer.count % Int(alignment)
//  if padding > 0 {
//    writer.append(Data([UInt8](repeating: 0, count: alignment - padding)))
//  }
//}

func getPadForAlignment(_ count: Int, alignment: Int = 8) -> Int {
  let padding = count % Int(alignment)
  if padding > 0 {
    return count + (alignment - padding)
  }
  return count
}
