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

protocol ArrowArrayProtocol<ItemType> {
  associatedtype ItemType
  subscript(_ index: Int) -> ItemType? { get }
  var offset: Int { get }
  var length: Int { get }
  func slice(offset: Int, length: Int) -> Self
  func any(at index: Int) -> Any?
}

// This exists to support type-erased struct arrays.
extension ArrowArrayProtocol {
  func any(at index: Int) -> Any? {
    self[index] as Any?
  }
}

/// An Arrow array of booleans using the three-valued logical model (true / false / null).
struct ArrowArrayBoolean: ArrowArrayProtocol {
  typealias ItemType = Bool
  let offset: Int
  let length: Int
  let nullBuffer: NullBuffer
  let valueBuffer: NullBuffer

  subscript(index: Int) -> Bool? {
    let offsetIndex = self.offset + index
    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }
    return valueBuffer.isSet(offsetIndex)
  }
  
  func slice(offset: Int, length: Int) -> ArrowArrayBoolean {
     return .init(
      offset: offset,
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// An Arrow array of fixed-width types.
struct ArrowArrayFixed<T>: ArrowArrayProtocol where T: Numeric {
  typealias ItemType = T
  let offset: Int
  let length: Int
  let nullBuffer: NullBuffer
  let valueBuffer: FixedWidthBuffer<T>

  subscript(index: Int) -> T? {
    let offsetIndex = self.offset + index
    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }
    return valueBuffer[offsetIndex]
  }
  
  func slice(offset: Int, length: Int) -> ArrowArrayFixed<T> {
    return .init(
      offset: offset,
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// An Arrow array of variable-length types.
struct ArrowArrayVariable<T>: ArrowArrayProtocol where T: VariableLength {
  typealias ItemType = T
  let offset: Int
  let length: Int
  let nullBuffer: NullBuffer
  let offsetsBuffer: FixedWidthBuffer<Int32>
  let valueBuffer: VariableLengthTypeBuffer<T>

  subscript(index: Int) -> T? {
    
    let offsetIndex = self.offset + index
    
    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }
    let startIndex = offsetsBuffer[offsetIndex]
    let endIndex = offsetsBuffer[offsetIndex + 1]
    return valueBuffer.loadVariable(
      at: Int(startIndex),
      arrayLength: Int(endIndex - startIndex)
    )
  }
  
  func slice(offset: Int, length: Int) -> Self {
    .init(
      offset: offset,
      length: length,
      nullBuffer: nullBuffer,
      offsetsBuffer: offsetsBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// An Arrow array of `Date`s with a resolution of 1 day.
struct ArrowArrayDate32: ArrowArrayProtocol {
  typealias ItemType = Date

  let array: ArrowArrayFixed<Date32>
  
  var offset: Int {
    array.offset
  }

  var length: Int {
    array.length
  }

  subscript(index: Int) -> Date? {
    let offsetIndex = self.offset + index
    let days: Int32? = array[offsetIndex]
    if let days {
      return Date(timeIntervalSince1970: TimeInterval(days * 86400))
    } else {
      return nil
    }
  }
  
  func slice(offset: Int, length: Int) -> Self {
    let internalSlice = array.slice(offset: offset, length: length)
    return .init(array: internalSlice)
  }
}

/// An Arrow array of `Date`s with a resolution of 1 second.
struct ArrowArrayDate64: ArrowArrayProtocol {
  typealias ItemType = Date

  let array: ArrowArrayFixed<Date64>
  
  var offset: Int {
    array.offset
  }

  var length: Int {
    array.length
  }

  subscript(index: Int) -> Date? {
    let offsetIndex = self.offset + index
    let milliseconds: Int64? = array[offsetIndex]
    if let milliseconds {
      return Date(timeIntervalSince1970: TimeInterval(milliseconds / 1000))
    } else {
      return nil
    }
  }
  
  func slice(offset: Int, length: Int) -> Self {
    let internalSlice = array.slice(offset: offset, length: length)
    return .init(array: internalSlice)
  }
}

struct ArrowListArray<T>: ArrowArrayProtocol where T: ArrowArrayProtocol {
  
  typealias ItemType = T
  
  let offset: Int
  let length: Int
  let nullBuffer: NullBuffer
  let offsetsBuffer: FixedWidthBuffer<Int32>
  let values: T
  
  subscript(index: Int) -> T? {
    let offsetIndex = self.offset + index
    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }
    let startIndex = offsetsBuffer[offsetIndex]
    let endIndex = offsetsBuffer[offsetIndex + 1]

    let length = endIndex - startIndex
    return values.slice(offset: Int(startIndex), length: Int(length))
  }
  
  func slice(offset: Int, length: Int) -> Self {
    .init(
      offset: self.offset + offset,
      length: length,
      nullBuffer: nullBuffer,
      offsetsBuffer: offsetsBuffer,
      values: values
    )
  }
}
