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

public class ChunkedArrayHolder {
  public let type: ArrowType
  public let length: UInt
  public let nullCount: UInt
  public let holder: Any
  
  public init<T>(_ chunked: ChunkedArray<T>) {
    self.holder = chunked
    self.length = chunked.length
    self.type = chunked.type
    self.nullCount = chunked.nullCount
  }
}

public class ChunkedArray<T> {
  public let arrays: [any ArrowArray<T>]
  public let type: ArrowType
  public let nullCount: UInt
  public let length: UInt
  public var arrayCount: UInt { UInt(self.arrays.count) }

  public init(_ arrays: [any ArrowArray<T>]) throws(ArrowError) {
    if arrays.count == 0 {
      throw ArrowError.arrayHasNoElements
    }

    self.type = arrays[0].type
    var len: UInt = 0
    var nullCount: UInt = 0
    for array in arrays {
      len += array.length
      nullCount += array.nullCount
    }

    self.arrays = arrays
    self.length = len
    self.nullCount = nullCount
  }

  public subscript(_ index: UInt) -> T? {
    if arrays.count == 0 {
      return nil
    }
    var localIndex = index
    var arrayIndex = 0
    var len: UInt = arrays[arrayIndex].length
    while localIndex > (len - 1) {
      arrayIndex += 1
      if arrayIndex > arrays.count {
        return nil
      }

      localIndex -= len
      len = arrays[arrayIndex].length
    }

    return arrays[arrayIndex][localIndex]
  }

  public func asString(_ index: UInt) -> String {
    guard let value = self[index] else {
      return ""
    }
    return "\(value)"
  }
}
