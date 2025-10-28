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
    let arrowType: ArrowType = .binary
    let arrowData = try ArrowData(
      arrowType, buffers: buffers, nullCount: nullCount)
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
    let arrowType: ArrowType = .utf8
    let arrowData = try ArrowData(
      arrowType, buffers: buffers, nullCount: nullCount)
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
    if field.type == .date32 {
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
    switch field.type {
    case .time32(_):
      let arrowData = try ArrowData(
        field.type, buffers: buffers, nullCount: nullCount)
      return .success(ArrowArrayHolderImpl(try FixedArray<Time32>(arrowData)))

    case .time64(_):
      let arrowData = try ArrowData(
        field.type, buffers: buffers, nullCount: nullCount)
      return .success(ArrowArrayHolderImpl(try FixedArray<Time64>(arrowData)))
    default:
      return .failure(
        .invalid("Incorrect field type for time: \(field.type)"))
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
    switch field.type {
    case .timestamp(_, _):
      let arrowData = try ArrowData(
        field.type, buffers: buffers, nullCount: nullCount)
      let array = try TimestampArray(arrowData)
      return .success(ArrowArrayHolderImpl(array))
    default:
      return .failure(
        .invalid("Incorrect field type for timestamp: \(field.type)"))
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
    let arrowData = try ArrowData(
      .boolean,
      buffers: buffers,
      nullCount: nullCount
    )
    return .success(ArrowArrayHolderImpl(try BoolArray(arrowData)))
  } catch {
    return .failure(error)
  }
}

private func makeFixedHolder<T>(
  _: T.Type,
  field: ArrowField,
  buffers: [ArrowBuffer],
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
  let typeId = field.type
  switch typeId {
  case .int8:
    return makeFixedHolder(
      Int8.self, field: field, buffers: buffers, nullCount: nullCount)
  case .int16:
    return makeFixedHolder(
      Int16.self, field: field, buffers: buffers, nullCount: nullCount)
  case .int32:
    return makeFixedHolder(
      Int32.self, field: field, buffers: buffers, nullCount: nullCount)
  case .int64:
    return makeFixedHolder(
      Int64.self, field: field, buffers: buffers, nullCount: nullCount)
  case .uint8:
    return makeFixedHolder(
      UInt8.self, field: field, buffers: buffers, nullCount: nullCount)
  case .uint16:
    return makeFixedHolder(
      UInt16.self, field: field, buffers: buffers, nullCount: nullCount)
  case .uint32:
    return makeFixedHolder(
      UInt32.self, field: field, buffers: buffers, nullCount: nullCount)
  case .uint64:
    return makeFixedHolder(
      UInt64.self, field: field, buffers: buffers, nullCount: nullCount)
  case .boolean:
    return makeBoolHolder(buffers, nullCount: nullCount)
  case .float32:
    return makeFixedHolder(
      Float.self, field: field, buffers: buffers, nullCount: nullCount)
  case .float64:
    return makeFixedHolder(
      Double.self, field: field, buffers: buffers, nullCount: nullCount)
  case .utf8:
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
    guard let children else {
      return .failure(.invalid("Expected a struct field to have children"))
    }
    return makeNestedHolder(
      field, buffers: buffers, nullCount: nullCount, children: children,
      rbLength: rbLength)
  case .list:
    guard let children else {
      return .failure(.invalid("Expected a list field to have children"))
    }
    return makeNestedHolder(
      field, buffers: buffers, nullCount: nullCount, children: children,
      rbLength: rbLength)
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

func validateFileData(_ data: Data) -> Bool {
  let markerLength = fileMarker.count
  let startString = data[..<markerLength]
  let endString = data[(data.count - markerLength)...]
  return startString == fileMarker && endString == fileMarker
}

func getUInt32(_ data: Data, offset: Int) -> UInt32 {
  let token = data.withUnsafeBytes { rawBuffer in
    rawBuffer.loadUnaligned(fromByteOffset: offset, as: UInt32.self)
  }
  return token
}
