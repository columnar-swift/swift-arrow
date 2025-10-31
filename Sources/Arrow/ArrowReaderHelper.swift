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
) -> Result<AnyArrowArray, ArrowError> {
  let arrowType: ArrowType = .binary
  let arrowData = ArrowData(
    arrowType,
    buffers: buffers,
    nullCount: nullCount
  )
  return .success(BinaryArray(arrowData))
}

private func makeStringHolder(
  _ buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<AnyArrowArray, ArrowError> {
  let arrowType: ArrowType = .utf8
  let arrowData = ArrowData(
    arrowType, buffers: buffers, nullCount: nullCount)
  return .success(StringArray(arrowData))
}

private func makeBoolHolder(
  _ buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<AnyArrowArray, ArrowError> {
  let arrowData = ArrowData(
    .boolean,
    buffers: buffers,
    nullCount: nullCount
  )
  return .success(BoolArray(arrowData))
}

private func makeFixedHolder<T: BitwiseCopyable>(
  _: T.Type,
  field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt
) -> FixedArray<T> {
  let arrowData = ArrowData(
    field.type,
    buffers: buffers,
    nullCount: nullCount
  )
  return FixedArray<T>(arrowData)
}

func makeNestedHolder(
  _ field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt,
  children: [ArrowData],
  rbLength: UInt
) -> Result<AnyArrowArray, ArrowError> {
  do {
    let arrowData = ArrowData(
      field.type,
      buffers: buffers,
      children: children,
      nullCount: nullCount,
      length: rbLength
    )
    return .success(try NestedArray(arrowData))
  } catch {
    return .failure(error)
  }
}

func makeArrayHolder(
  _ field: FlatField,
  buffers: [ArrowBuffer],
  nullCount: UInt,
  children: [ArrowData]?,
  rbLength: UInt
) -> Result<AnyArrowArray, ArrowError> {
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
) -> Result<AnyArrowArray, ArrowError> {

  let typeId = field.type
  switch typeId {
  case .int8:
    return .success(
      makeFixedHolder(
        Int8.self, field: field, buffers: buffers, nullCount: nullCount)
    )
  case .int16:
    return .success(
      makeFixedHolder(
        Int16.self, field: field, buffers: buffers, nullCount: nullCount)
    )
  case .int32:
    return .success(
      makeFixedHolder(
        Int32.self, field: field, buffers: buffers, nullCount: nullCount)
    )
  case .int64:
    return .success(
      makeFixedHolder(
        Int64.self, field: field, buffers: buffers, nullCount: nullCount)
    )
  case .uint8:
    return .success(
      makeFixedHolder(
        UInt8.self, field: field, buffers: buffers, nullCount: nullCount)
    )
  case .uint16:
    return .success(
      makeFixedHolder(
        UInt16.self, field: field, buffers: buffers, nullCount: nullCount)
    )
  case .uint32:
    return .success(
      makeFixedHolder(
        UInt32.self, field: field, buffers: buffers, nullCount: nullCount)
    )
  case .uint64:
    return .success(
      makeFixedHolder(
        UInt64.self, field: field, buffers: buffers, nullCount: nullCount)
    )
  case .boolean:
    return makeBoolHolder(buffers, nullCount: nullCount)
  case .float32:
    return .success(
      makeFixedHolder(
        Float.self, field: field, buffers: buffers, nullCount: nullCount)
    )
  case .float64:
    return .success(
      makeFixedHolder(
        Double.self, field: field, buffers: buffers, nullCount: nullCount)
    )
  case .utf8:
    return makeStringHolder(buffers, nullCount: nullCount)
  case .binary:
    return makeBinaryHolder(buffers, nullCount: nullCount)
  case .date32:
    let arrowData = ArrowData(
      field.type,
      buffers: buffers,
      nullCount: nullCount
    )
    return .success(Date32Array(arrowData))
  case .date64:
    let arrowData = ArrowData(
      field.type,
      buffers: buffers,
      nullCount: nullCount
    )
    return .success(Date64Array(arrowData))
  case .time32:
    let arrowData = ArrowData(
      field.type, buffers: buffers, nullCount: nullCount)
    return .success(FixedArray<Time32>(arrowData))
  case .time64:
    let arrowData = ArrowData(
      field.type, buffers: buffers, nullCount: nullCount)
    return .success(FixedArray<Time64>(arrowData))
  case .timestamp:
    let arrowData = ArrowData(
      field.type, buffers: buffers, nullCount: nullCount)
    return .success(TimestampArray(arrowData))
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



func getUInt32(_ data: Data, offset: Int) -> UInt32 {
  let token = data.withUnsafeBytes { rawBuffer in
    rawBuffer.loadUnaligned(fromByteOffset: offset, as: UInt32.self)
  }
  return token
}
