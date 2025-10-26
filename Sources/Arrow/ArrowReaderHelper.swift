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

private func makeBinaryHolder(
  _ buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    let arrowType = ArrowType(ArrowType.arrowBinary)
    let arrowData = try ArrowData(arrowType, buffers: buffers, nullCount: nullCount)
    return .success(ArrowArrayHolderImpl(try BinaryArray(arrowData)))
  } catch {
    return .failure(error)
  }
}

private func makeStringHolder(
  _ buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    let arrowType = ArrowType(ArrowType.arrowString)
    let arrowData = try ArrowData(arrowType, buffers: buffers, nullCount: nullCount)
    return .success(ArrowArrayHolderImpl(try StringArray(arrowData)))
  } catch {
    return .failure(error)
  }
}

private func makeDateHolder(
  _ field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    if field.type.id == .date32 {
      let arrowData = try ArrowData(
        field.type,
        buffers: buffers,
        nullCount: nullCount
      )
      return .success(ArrowArrayHolderImpl(try Date32Array(arrowData)))
    }
    let arrowData = try ArrowData(
      field.type,
      buffers: buffers,
      nullCount: nullCount
    )
    return .success(ArrowArrayHolderImpl(try Date64Array(arrowData)))
  } catch {
    return .failure(error)
  }
}

private func makeTimeHolder(
  _ field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    if field.type.id == .time32 {
      if let arrowType = field.type as? ArrowTypeTime32 {
        let arrowData = try ArrowData(arrowType, buffers: buffers, nullCount: nullCount)
        return .success(ArrowArrayHolderImpl(try FixedArray<Time32>(arrowData)))
      } else {
        return .failure(.invalid("Incorrect field type for time: \(field.type)"))
      }
    }
    if let arrowType = field.type as? ArrowTypeTime64 {
      let arrowData = try ArrowData(arrowType, buffers: buffers, nullCount: nullCount)
      return .success(ArrowArrayHolderImpl(try FixedArray<Time64>(arrowData)))
    } else {
      return .failure(.invalid("Incorrect field type for time: \(field.type)"))
    }
  } catch {
    return .failure(error)
  }
}

private func makeTimestampHolder(
  _ field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    if let arrowType = field.type as? ArrowTypeTimestamp {
      let arrowData = try ArrowData(arrowType, buffers: buffers, nullCount: nullCount)
      return .success(ArrowArrayHolderImpl(try TimestampArray(arrowData)))
    } else {
      return .failure(.invalid("Incorrect field type for timestamp: \(field.type)"))
    }
  } catch {
    return .failure(error)
  }
}

private func makeBoolHolder(
  _ buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    let arrowType = ArrowType(ArrowType.arrowBool)
    let arrowData = try ArrowData(
      arrowType,
      buffers: buffers,
      nullCount: nullCount
    )
    return .success(ArrowArrayHolderImpl(try BoolArray(arrowData)))
  } catch {
    return .failure(error)
  }
}

private func makeFixedHolder<T>(
  _: T.Type, field: ArrowField, buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    let arrowData = try ArrowData(
      field.type,
      buffers: buffers,
      nullCount: nullCount
    )
    return .success(ArrowArrayHolderImpl(try FixedArray<T>(arrowData)))
  } catch {
    return .failure(error)
  }
}

func makeNestedHolder(
  _ field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt,
  children: [ArrowData],
  rbLength: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    let arrowData = try ArrowData(
      field.type,
      buffers: buffers,
      children: children,
      nullCount: nullCount,
      length: rbLength
    )
    return .success(ArrowArrayHolderImpl(try NestedArray(arrowData)))
  } catch {
    return .failure(error)
//  } catch {
//    return .failure(.unknownError("\(error)"))
  }
}

func makeArrayHolder(
  _ field: FlatField,
  buffers: [ArrowBuffer],
  nullCount: UInt,
  children: [ArrowData]?,
  rbLength: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    let arrowField = try fromProto(field: field)
    return makeArrayHolder(
      arrowField,
      buffers: buffers,
      nullCount: nullCount,
      children: children,
      rbLength: rbLength
    )
  } catch {
    return .failure(error)
  }
}

func makeArrayHolder(
  _ field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt,
  children: [ArrowData]?,
  rbLength: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  let typeId = field.type.id
  switch typeId {
  case .int8:
    return makeFixedHolder(Int8.self, field: field, buffers: buffers, nullCount: nullCount)
  case .uint8:
    return makeFixedHolder(UInt8.self, field: field, buffers: buffers, nullCount: nullCount)
  case .int16:
    return makeFixedHolder(Int16.self, field: field, buffers: buffers, nullCount: nullCount)
  case .uint16:
    return makeFixedHolder(UInt16.self, field: field, buffers: buffers, nullCount: nullCount)
  case .int32:
    return makeFixedHolder(Int32.self, field: field, buffers: buffers, nullCount: nullCount)
  case .uint32:
    return makeFixedHolder(UInt32.self, field: field, buffers: buffers, nullCount: nullCount)
  case .int64:
    return makeFixedHolder(Int64.self, field: field, buffers: buffers, nullCount: nullCount)
  case .uint64:
    return makeFixedHolder(UInt64.self, field: field, buffers: buffers, nullCount: nullCount)
  case .boolean:
    return makeBoolHolder(buffers, nullCount: nullCount)
  case .float:
    return makeFixedHolder(Float.self, field: field, buffers: buffers, nullCount: nullCount)
  case .double:
    return makeFixedHolder(Double.self, field: field, buffers: buffers, nullCount: nullCount)
  case .string:
    return makeStringHolder(buffers, nullCount: nullCount)
  case .binary:
    return makeBinaryHolder(buffers, nullCount: nullCount)
  case .date32, .date64:
    return makeDateHolder(field, buffers: buffers, nullCount: nullCount)
  case .time32, .time64:
    return makeTimeHolder(field, buffers: buffers, nullCount: nullCount)
  case .timestamp:
    return makeTimestampHolder(field, buffers: buffers, nullCount: nullCount)
  case .strct:
    return makeNestedHolder(
      field, buffers: buffers, nullCount: nullCount, children: children!, rbLength: rbLength)
  case .list:
    return makeNestedHolder(
      field, buffers: buffers, nullCount: nullCount, children: children!, rbLength: rbLength)
  default:
    return .failure(.unknownType("Type \(typeId) currently not supported"))
  }
}

func makeBuffer(
  _ buffer: org_apache_arrow_flatbuf_Buffer, fileData: Data,
  length: UInt, messageOffset: Int64
) -> ArrowBuffer {
  let startOffset = messageOffset + buffer.offset
  let endOffset = startOffset + buffer.length
  let bufferData = [UInt8](fileData[startOffset..<endOffset])
  return ArrowBuffer.createBuffer(bufferData, length: length)
}

func isFixedPrimitive(_ type: org_apache_arrow_flatbuf_Type_) -> Bool {
  switch type {
  case .int, .bool, .floatingpoint, .date, .time, .timestamp:
    return true
  default:
    return false
  }
}

func findArrowType(_ field: FlatField) -> ArrowType {
  let type = field.typeType
  switch type {
  case .int:
    let intType = field.type(type: FlatInt.self)!
    let bitWidth = intType.bitWidth
    if bitWidth == 8 {
      return ArrowType(intType.isSigned ? ArrowType.arrowInt8 : ArrowType.arrowUInt8)
    }
    if bitWidth == 16 {
      return ArrowType(intType.isSigned ? ArrowType.arrowInt16 : ArrowType.arrowUInt16)
    }
    if bitWidth == 32 {
      return ArrowType(intType.isSigned ? ArrowType.arrowInt32 : ArrowType.arrowUInt32)
    }
    if bitWidth == 64 {
      return ArrowType(intType.isSigned ? ArrowType.arrowInt64 : ArrowType.arrowUInt64)
    }
    return ArrowType(ArrowType.arrowUnknown)
  case .bool:
    return ArrowType(ArrowType.arrowBool)
  case .floatingpoint:
    let floatType = field.type(type: FloatingPoint.self)!
    switch floatType.precision {
    case .single:
      return ArrowType(ArrowType.arrowFloat)
    case .double:
      return ArrowType(ArrowType.arrowDouble)
    default:
      return ArrowType(ArrowType.arrowUnknown)
    }
  case .utf8:
    return ArrowType(ArrowType.arrowString)
  case .binary:
    return ArrowType(ArrowType.arrowBinary)
  case .date:
    let dateType = field.type(type: org_apache_arrow_flatbuf_Date.self)!
    if dateType.unit == .day {
      return ArrowType(ArrowType.arrowDate32)
    }
    return ArrowType(ArrowType.arrowDate64)
  case .time:
    let timeType = field.type(type: FlatTime.self)!
    if timeType.unit == .second || timeType.unit == .millisecond {
      return ArrowTypeTime32(timeType.unit == .second ? .seconds : .milliseconds)
    }
    return ArrowTypeTime64(timeType.unit == .microsecond ? .microseconds : .nanoseconds)
  case .timestamp:
    let timestampType = field.type(type: org_apache_arrow_flatbuf_Timestamp.self)!
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
    return ArrowTypeTimestamp(arrowUnit, timezone: timezone)
  case .struct_:
    _ = field.type(type: FlatStruct.self)!
    var fields = [ArrowField]()
    for index in 0..<field.childrenCount {
      let childField = field.children(at: index)!
      let childType = findArrowType(childField)
      fields.append(
        ArrowField(childField.name ?? "", type: childType, isNullable: childField.nullable))
    }
    return ArrowTypeStruct(ArrowType.arrowStruct, fields: fields)
  case .list:
    guard field.childrenCount == 1, let childField = field.children(at: 0) else {
      return ArrowType(ArrowType.arrowUnknown)
    }
    let childType = findArrowType(childField)
    return ArrowTypeList(childType)
  default:
    return ArrowType(ArrowType.arrowUnknown)
  }
}

func validateBufferIndex(_ recordBatch: FlatRecordBatch, index: Int32) throws {
  if index >= recordBatch.buffersCount {
    throw ArrowError.outOfBounds(index: Int64(index))
  }
}

func validateFileData(_ data: Data) -> Bool {
  let markerLength = fileMarker.utf8.count
  let startString = String(decoding: data[..<markerLength], as: UTF8.self)
  let endString = String(decoding: data[(data.count - markerLength)...], as: UTF8.self)
  return startString == fileMarker && endString == fileMarker
}

func getUInt32(_ data: Data, offset: Int) -> UInt32 {
  let token = data.withUnsafeBytes { rawBuffer in
    rawBuffer.loadUnaligned(fromByteOffset: offset, as: UInt32.self)
  }
  return token
}
