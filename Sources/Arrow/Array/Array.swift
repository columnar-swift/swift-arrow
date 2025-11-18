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

public protocol ArrowArrayProtocol {
  associatedtype ItemType
  subscript(_ index: Int) -> ItemType? { get }
  var offset: Int { get }
  var length: Int { get }
  func slice(offset: Int, length: Int) -> Self
  func any(at index: Int) -> Any?
}

// This exists to support type-erased struct arrays.
extension ArrowArrayProtocol {
  public func any(at index: Int) -> Any? {
    self[index] as Any?
  }
}

/// An Arrow array of booleans using the three-valued logical model (true / false / null).
public struct ArrowArrayBoolean: ArrowArrayProtocol {
  public typealias ItemType = Bool
  public let offset: Int
  public let length: Int
  let nullBuffer: NullBuffer
  let valueBuffer: NullBuffer

  public init(
    offset: Int,
    length: Int,
    nullBuffer: NullBuffer,
    valueBuffer: NullBuffer
  ) {
    self.offset = offset
    self.length = length
    self.nullBuffer = nullBuffer
    self.valueBuffer = valueBuffer
  }

  public subscript(index: Int) -> Bool? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }
    return valueBuffer.isSet(offsetIndex)
  }

  public func slice(offset: Int, length: Int) -> ArrowArrayBoolean {
    .init(
      offset: offset,
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// An Arrow array of fixed-width types.
public struct ArrowArrayFixed<ValueBuffer>: ArrowArrayProtocol
where
  ValueBuffer: FixedWidthBufferProtocol,
  ValueBuffer.ElementType: Numeric
{
  public typealias ItemType = ValueBuffer.ElementType

  //  public typealias ItemType = Element
  public let offset: Int
  public let length: Int
  let nullBuffer: NullBuffer
  let valueBuffer: ValueBuffer

  public init(
    offset: Int = 0,
    length: Int,
    nullBuffer: NullBuffer,
    valueBuffer: ValueBuffer
  ) {
    self.offset = offset
    self.length = length
    self.nullBuffer = nullBuffer
    self.valueBuffer = valueBuffer
  }

  public subscript(index: Int) -> ValueBuffer.ElementType? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    if !self.nullBuffer.isSet(offsetIndex) {
      return nil
    }
    return valueBuffer[offsetIndex]
  }

  public func slice(offset: Int, length: Int) -> Self {
    .init(
      offset: offset,
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// An Arrow array of variable-length types.
public struct ArrowArrayVariable<Element, OffsetsBuffer, ValueBuffer>:
  ArrowArrayProtocol
where
  Element: VariableLength,
  OffsetsBuffer: FixedWidthBufferProtocol<Int32>,
  ValueBuffer: VariableLengthBufferProtocol<Element>
{
  public typealias ItemType = Element
  public let offset: Int
  public let length: Int
  let nullBuffer: NullBuffer
  let offsetsBuffer: OffsetsBuffer
  let valueBuffer: ValueBuffer

  public init(
    offset: Int = 0,
    length: Int,
    nullBuffer: NullBuffer,
    offsetsBuffer: OffsetsBuffer,
    valueBuffer: ValueBuffer
  ) {
    self.offset = offset
    self.length = length
    self.nullBuffer = nullBuffer
    self.offsetsBuffer = offsetsBuffer
    self.valueBuffer = valueBuffer
  }

  public subscript(index: Int) -> Element? {

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

  public func slice(offset: Int, length: Int) -> Self {
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
struct ArrowArrayDate32<ValueBuffer>: ArrowArrayProtocol
where
  ValueBuffer: FixedWidthBufferProtocol<Int32>
{
  typealias ItemType = Date

  let array: ArrowArrayFixed<ValueBuffer>

  var offset: Int {
    array.offset
  }

  var length: Int {
    array.length
  }

  subscript(index: Int) -> Date? {
    precondition(index >= 0 && index < length, "Invalid index.")
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
struct ArrowArrayDate64<ValueBuffer>: ArrowArrayProtocol
where
  ValueBuffer: FixedWidthBufferProtocol<Date64>
{
  typealias ItemType = Date

  let array: ArrowArrayFixed<ValueBuffer>

  var offset: Int {
    array.offset
  }

  var length: Int {
    array.length
  }

  subscript(index: Int) -> Date? {
    precondition(index >= 0 && index < length, "Invalid index.")
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

/// An Arrow list array which may be nested arbitrarily.
struct ArrowListArray<Element>: ArrowArrayProtocol
where
  Element: ArrowArrayProtocol
{
  typealias ItemType = Element

  let offset: Int
  let length: Int
  let nullBuffer: NullBuffer
  let offsetsBuffer: FixedWidthBuffer<Int32>
  let values: Element

  subscript(index: Int) -> Element? {
    precondition(index >= 0 && index < length, "Invalid index.")
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

/// An Arrow struct array.
public struct ArrowStructArray: ArrowArrayProtocol {
  public typealias ItemType = [String: Any]

  let nullBuffer: NullBuffer
  public let offset: Int
  public let length: Int
  public let fields: [(name: String, array: any ArrowArrayProtocol)]

  public init(
    offset: Int = 0,
    length: Int,
    nullBuffer: NullBuffer,
    fields: [(name: String, array: any ArrowArrayProtocol)]
  ) {
    self.offset = offset
    self.length = length
    self.nullBuffer = nullBuffer
    self.fields = fields
  }

  public subscript(index: Int) -> ItemType? {
    guard nullBuffer.isSet(offset + index) else { return nil }

    var result: [String: Any] = [:]
    for (name, array) in fields {
      result[name] = array.any(at: index)
    }
    return result
  }

  public func slice(offset newOffset: Int, length newLength: Int) -> Self {
    .init(
      offset: self.offset + newOffset,
      length: newLength,
      nullBuffer: nullBuffer,
      fields: fields
    )
  }
}
