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

public protocol AnyArrowArrayProtocol {
  var offset: Int { get }
  var length: Int { get }
  var nullCount: Int { get }
  func slice(offset: Int, length: Int) -> Self
  func any(at index: Int) -> Any?
  var bufferSizes: [Int] { get }
  var buffers: [ArrowBufferProtocol] { get }
}

internal protocol ArrowArrayProtocol: AnyArrowArrayProtocol {
  associatedtype ItemType
  subscript(_ index: Int) -> ItemType? { get }
}

// This exists to support type-erased struct arrays.
extension ArrowArrayProtocol {
  public func any(at index: Int) -> Any? {
    self[index] as Any?
  }
}

public protocol ArrowArrayOfString {
  subscript(index: Int) -> String? { get }
}
extension ArrowArrayVariable: ArrowArrayOfString where ItemType == String {}

/// An Arrow array of booleans using the three-valued logical model (true / false / null).
public struct ArrowArrayBoolean: ArrowArrayProtocol {
  public typealias ItemType = Bool
  public let offset: Int
  public let length: Int
  public var bufferSizes: [Int] { [nullBuffer.length, valueBuffer.length] }
  public var buffers: [ArrowBufferProtocol] { [nullBuffer, valueBuffer] }
  public var nullCount: Int { nullBuffer.nullCount }
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
  public let offset: Int
  public let length: Int
  public var bufferSizes: [Int] { [nullBuffer.length, valueBuffer.length] }
  public var buffers: [ArrowBufferProtocol] { [nullBuffer, valueBuffer] }
  public var nullCount: Int { nullBuffer.nullCount }
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
public struct ArrowArrayVariable<OffsetsBuffer, ValueBuffer>:
  ArrowArrayProtocol
where
  OffsetsBuffer: FixedWidthBufferProtocol<Int32>,
  ValueBuffer: VariableLengthBufferProtocol<ValueBuffer.ElementType>,
  ValueBuffer.ElementType: VariableLength
{
  public typealias ItemType = ValueBuffer.ElementType
  public let offset: Int
  public let length: Int
  public var bufferSizes: [Int] {
    [nullBuffer.length, offsetsBuffer.length, valueBuffer.length]
  }
  public var buffers: [ArrowBufferProtocol] {
    [nullBuffer, offsetsBuffer, valueBuffer]
  }
  public var nullCount: Int { nullBuffer.nullCount }
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

  public subscript(index: Int) -> ValueBuffer.ElementType? {
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
public struct ArrowArrayDate32<ValueBuffer>: ArrowArrayProtocol
where
  ValueBuffer: FixedWidthBufferProtocol<Int32>
{
  public typealias ItemType = Date
  public var bufferSizes: [Int] { array.bufferSizes }
  public var buffers: [ArrowBufferProtocol] { array.buffers }
  public var nullCount: Int { array.nullCount }
  public var offset: Int { array.offset }
  public var length: Int { array.length }
  let array: ArrowArrayFixed<ValueBuffer>

  public subscript(index: Int) -> Date? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    let days: Int32? = array[offsetIndex]
    if let days {
      return Date(timeIntervalSince1970: TimeInterval(days * 86400))
    } else {
      return nil
    }
  }

  public func slice(offset: Int, length: Int) -> Self {
    let internalSlice = array.slice(offset: offset, length: length)
    return .init(array: internalSlice)
  }
}

/// An Arrow array of `Date`s with a resolution of 1 second.
public struct ArrowArrayDate64<ValueBuffer>: ArrowArrayProtocol
where
  ValueBuffer: FixedWidthBufferProtocol<Date64>
{
  public typealias ItemType = Date
  public var bufferSizes: [Int] { array.bufferSizes }
  public var buffers: [ArrowBufferProtocol] { array.buffers }
  public var nullCount: Int { array.nullCount }
  public var offset: Int { array.offset }
  public var length: Int { array.length }
  let array: ArrowArrayFixed<ValueBuffer>

  public subscript(index: Int) -> Date? {
    precondition(index >= 0 && index < length, "Invalid index.")
    let offsetIndex = self.offset + index
    let milliseconds: Int64? = array[offsetIndex]
    if let milliseconds {
      return Date(timeIntervalSince1970: TimeInterval(milliseconds / 1000))
    } else {
      return nil
    }
  }

  public func slice(offset: Int, length: Int) -> Self {
    let internalSlice = array.slice(offset: offset, length: length)
    return .init(array: internalSlice)
  }
}

/// A strongly-typed Arrow list array which may be nested arbitrarily.
public struct ArrowListArray<Element, OffsetsBuffer>: ArrowArrayProtocol
where
  OffsetsBuffer: FixedWidthBufferProtocol<Int32>,
  Element: AnyArrowArrayProtocol
{
  public typealias ItemType = Element
  public let offset: Int
  public let length: Int
  public var bufferSizes: [Int] {
    [nullBuffer.length, offsetsBuffer.length]
  }
  public var buffers: [ArrowBufferProtocol] {
    [nullBuffer, offsetsBuffer]
  }
  public var nullCount: Int { nullBuffer.nullCount }
  let nullBuffer: NullBuffer
  let offsetsBuffer: OffsetsBuffer
  let values: Element

  public init(
    offset: Int = 0,
    length: Int,
    nullBuffer: NullBuffer,
    offsetsBuffer: OffsetsBuffer,
    values: Element
  ) {
    self.offset = offset
    self.length = length
    self.nullBuffer = nullBuffer
    self.offsetsBuffer = offsetsBuffer
    self.values = values
  }

  public subscript(index: Int) -> Element? {
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

  public func slice(offset: Int, length: Int) -> Self {
    .init(
      offset: self.offset + offset,
      length: length,
      nullBuffer: nullBuffer,
      offsetsBuffer: offsetsBuffer,
      values: values
    )
  }
}

/// A type-erased wrapper for an Arrow list array.
public struct AnyArrowListArray: ArrowArrayProtocol {

  public typealias ItemType = AnyArrowArrayProtocol
  public var bufferSizes: [Int] {
    _base.bufferSizes
  }
  public var buffers: [ArrowBufferProtocol] {
    _base.buffers
  }

  private let _base: any ArrowArrayProtocol
  private let _subscriptImpl: (Int) -> AnyArrowArrayProtocol?
  private let _sliceImpl: (Int, Int) -> AnyArrowListArray

  public let offset: Int
  public let length: Int
  public var nullCount: Int { _base.nullCount }

  init<Element, OffsetsBuffer>(
    _ list: ArrowListArray<Element, OffsetsBuffer>
  )
  where
    OffsetsBuffer: FixedWidthBufferProtocol<Int32>,
    Element: ArrowArrayProtocol
  {
    self._base = list
    self.offset = list.offset
    self.length = list.length
    self._subscriptImpl = { list[$0] }
    self._sliceImpl = { AnyArrowListArray(list.slice(offset: $0, length: $1)) }
  }

  public subscript(index: Int) -> AnyArrowArrayProtocol? {
    _subscriptImpl(index)
  }

  public func slice(offset: Int, length: Int) -> AnyArrowListArray {
    _sliceImpl(offset, length)
  }
}

/// An Arrow struct array.
public struct ArrowStructArray: ArrowArrayProtocol {
  public typealias ItemType = [String: Any]
  public let offset: Int
  public let length: Int
  public let fields: [(name: String, array: AnyArrowArrayProtocol)]
  public var bufferSizes: [Int] { [nullBuffer.length] }
  public var buffers: [ArrowBufferProtocol] { [nullBuffer] }
  public var nullCount: Int { nullBuffer.nullCount }
  let nullBuffer: NullBuffer

  public init(
    offset: Int = 0,
    length: Int,
    nullBuffer: NullBuffer,
    fields: [(name: String, array: AnyArrowArrayProtocol)]
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
