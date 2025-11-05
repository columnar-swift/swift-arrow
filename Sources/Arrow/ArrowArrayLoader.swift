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

// TODO: Duplicated
struct ArrowArrayLoader {
  public static func loadArray(
    _ arrowType: ArrowType,
    with arrowData: ArrowData
  ) throws(ArrowError) -> any AnyArrowArray {
    switch arrowType {
    case .int8:
      return try FixedArray<Int8>(arrowData)
    case .int16:
      return try FixedArray<Int16>(arrowData)
    case .int32:
      return try FixedArray<Int32>(arrowData)
    case .int64:
      return try FixedArray<Int64>(arrowData)
    case .uint8:
      return try FixedArray<UInt8>(arrowData)
    case .uint16:
      return try FixedArray<UInt16>(arrowData)
    case .uint32:
      return try FixedArray<UInt32>(arrowData)
    case .uint64:
      return try FixedArray<UInt64>(arrowData)
    case .float64:
      return try FixedArray<Double>(arrowData)
    case .float32:
      return try FixedArray<Float>(arrowData)
    case .date32:
      return try Date32Array(arrowData)
    case .date64:
      return try Date64Array(arrowData)
    case .time32:
      return try Time32Array(arrowData)
    case .time64:
      return try Time64Array(arrowData)
    case .timestamp:
      return try TimestampArray(arrowData)
    case .utf8:
      return try StringArray(arrowData)
    case .boolean:
      return try BoolArray(arrowData)
    case .binary:
      return try BinaryArray(arrowData)
    case .strct(let _):
      return try NestedArray(arrowData)
    case .list(let _):
      return try NestedArray(arrowData)
    default:
      throw .invalid("Array not found for type: \(arrowType)")
    }
  }

}
