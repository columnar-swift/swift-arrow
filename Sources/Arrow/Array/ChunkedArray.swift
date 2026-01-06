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

public protocol ChunkedArrayProtocol {
  var length: Int { get }
  var nullCount: Int { get }
}

public class ChunkedArray<T>: ChunkedArrayProtocol {
  public let arrays: [any ArrowArrayProtocol<T>]
  public let nullCount: Int
  public let length: Int

  public init(_ arrays: [any ArrowArrayProtocol<T>]) throws(ArrowError) {
    if arrays.count == 0 {
      throw ArrowError(.arrayHasNoElements)
    }

    var len: Int = 0
    var nullCount: Int = 0
    for array in arrays {
      len += array.length
      nullCount += array.nullCount
    }

    self.arrays = arrays
    self.length = len
    self.nullCount = nullCount
  }

  public subscript(_ index: Int) -> T? {
    if arrays.count == 0 {
      return nil
    }
    var localIndex = index
    var arrayIndex = 0
    var len: Int = arrays[arrayIndex].length
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

  public func asString(_ index: Int) -> String {
    guard let value = self[index] else {
      return ""
    }
    return "\(value)"
  }
}

/// A type-erased chunked array, suitable for complex types.
public final class AnyChunkedArray: ChunkedArrayProtocol {
  private let arrays: [any AnyArrowArrayProtocol]
  public let nullCount: Int
  public let length: Int

  // Cached chunk boundaries
  private let chunkOffsets: [Int]

  public init(_ arrays: [any AnyArrowArrayProtocol]) throws(ArrowError) {
    guard !arrays.isEmpty else {
      throw ArrowError(.arrayHasNoElements)
    }

    var len: Int = 0
    var nullCount: Int = 0
    var offsets: [Int] = [0]

    for array in arrays {
      len += array.length
      nullCount += array.nullCount
      offsets.append(len)
    }

    self.arrays = arrays
    self.length = len
    self.nullCount = nullCount
    self.chunkOffsets = offsets
  }

  public subscript(_ index: Int) -> Any? {
    guard index >= 0, index < length else {
      return nil
    }
    // Binary search to find the right chunk
    var low = 0
    var high = arrays.count - 1

    while low <= high {
      let mid = (low + high) / 2
      let chunkStart = chunkOffsets[mid]
      let chunkEnd = chunkOffsets[mid + 1]

      if index < chunkStart {
        high = mid - 1
      } else if index >= chunkEnd {
        low = mid + 1
      } else {
        // Found the right chunk
        let localIndex = index - chunkStart
        return arrays[mid].any(at: localIndex)
      }
    }
    return nil
  }

  public func any(at index: Int) -> Any? {
    self[index]
  }

  public func asString(_ index: Int) -> String {
    guard let value = self[index] else {
      return ""
    }
    return "\(value)"
  }
}
