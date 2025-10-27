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

import Foundation

public struct ArrowTypeConverter {

  public static func infoForType(
    _ type: Any.Type
  ) throws(ArrowError) -> ArrowType {
    if type == String.self {
      return .utf8
    } else if type == Date.self {
      return .date64
    } else if type == Bool.self {
      return .boolean
    } else if type == Data.self {
      return .binary
    } else if type == Int8.self {
      return .int8
    } else if type == Int16.self {
      return .int16
    } else if type == Int32.self {
      return .int32
    } else if type == Int64.self {
      return .int64
    } else if type == UInt8.self {
      return .uint8
    } else if type == UInt16.self {
      return .uint16
    } else if type == UInt32.self {
      return .uint32
    } else if type == UInt64.self {
      return .uint64
    } else if type == Float.self {
      return .float32
    } else if type == Double.self {
      return .float64
    } else {
      throw .invalid("Unsupported type: \(type)")
    }
  }

  public static func infoForNumericType<T>(
    _ type: T.Type
  ) throws(ArrowError) -> ArrowType {
    if type == Int8.self {
      return .int8
    } else if type == Int16.self {
      return .int16
    } else if type == Int32.self {
      return .int32
    } else if type == Int64.self {
      return .int64
    } else if type == UInt8.self {
      return .uint8
    } else if type == UInt16.self {
      return .uint16
    } else if type == UInt32.self {
      return .uint32
    } else if type == UInt64.self {
      return .uint64
    } else if type == Float.self {
      return .float32
    } else if type == Double.self {
      return .float64
    } else {
      throw .invalid("Unsupported numeric type: \(type)")
    }
  }
}
