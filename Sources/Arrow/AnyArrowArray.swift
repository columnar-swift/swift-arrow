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

// FIXME: Temporary to support holder refactor
struct ArrowArrayLoader {
  public static func loadArray(
    _ arrowType: ArrowType,
    with arrowData: ArrowData
  ) throws(ArrowError) -> any AnyArrowArray {
    switch arrowType {
    case .int8:
      return FixedArray<Int8>(arrowData)
    case .int16:
      return FixedArray<Int16>(arrowData)
    case .int32:
      return FixedArray<Int32>(arrowData)
    case .int64:
      return FixedArray<Int64>(arrowData)
    case .uint8:
      return FixedArray<UInt8>(arrowData)
    case .uint16:
      return FixedArray<UInt16>(arrowData)
    case .uint32:
      return FixedArray<UInt32>(arrowData)
    case .uint64:
      return FixedArray<UInt64>(arrowData)
    case .float64:
      return FixedArray<Double>(arrowData)
    case .float32:
      return FixedArray<Float>(arrowData)
    case .date32:
      return Date32Array(arrowData)
    case .date64:
      return Date64Array(arrowData)
    case .time32:
      return Time32Array(arrowData)
    case .time64:
      return Time64Array(arrowData)
    case .timestamp:
      return TimestampArray(arrowData)
    case .utf8:
      return StringArray(arrowData)
    case .boolean:
      return BoolArray(arrowData)
    case .binary:
      return BinaryArray(arrowData)
    case .strct(let _):
      return try NestedArray(arrowData)
    case .list(let _):
      return try NestedArray(arrowData)
    default:
      throw .invalid("Array not found for type: \(arrowType)")
    }
  }

}
