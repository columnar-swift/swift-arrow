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

import Foundation

public protocol ByteConvertible {
  func toData() -> Data
}

extension String: ByteConvertible {
  public func toData() -> Data {
    Data(self.utf8)
  }
}

extension Data: ByteConvertible {
  public func toData() -> Data {
    self
  }
}

public protocol ArrowBufferBuilder {
  associatedtype ItemType
  var capacity: UInt { get }
  var length: UInt { get }
  var nullCount: UInt { get }
  var offset: UInt { get }
  init() throws(ArrowError)
  func append(_ newValue: ItemType?)
  func isNull(_ index: UInt) -> Bool
  func resize(_ length: UInt)
  func finish() -> [ArrowBuffer]
}

public class BaseBufferBuilder {
  var nulls: ArrowBuffer
  public var offset: UInt = 0
  public var capacity: UInt { self.nulls.capacity }
  public var length: UInt = 0
  public var nullCount: UInt = 0

  init(_ nulls: ArrowBuffer) {
    self.nulls = nulls
  }

  public func isNull(_ index: UInt) -> Bool {
    self.nulls.length == 0
      || BitUtility.isSet(index + self.offset, buffer: self.nulls)
  }

  func resizeLength(_ data: ArrowBuffer, len: UInt = 0) -> UInt {
    if len == 0 || len < data.length * 2 {
      if data.length == 0 || data.length * 2 < ArrowBuffer.minLength {
        return ArrowBuffer.minLength
      }
      return UInt(data.length * 2)
    }
    return UInt(len * 2)
  }
}

public class ValuesBufferBuilder<T>: BaseBufferBuilder {
  var values: ArrowBuffer
  var stride: Int
  public override var capacity: UInt { self.values.capacity }

  init(
    values: ArrowBuffer, nulls: ArrowBuffer,
    stride: Int = MemoryLayout<T>.stride
  ) {
    self.stride = stride
    self.values = values
    super.init(nulls)
  }
}

// TODO: look at potential for typed memory allocation
/// Builds buffers of fixed-width types.
public class FixedBufferBuilder<T>: ValuesBufferBuilder<T>, ArrowBufferBuilder
where T: Numeric {
  public typealias ItemType = T
  private let defaultVal: ItemType = 0

  public required init() throws(ArrowError) {
    let values = ArrowBuffer.createBuffer(0, size: UInt(MemoryLayout<T>.stride))
    let nulls = ArrowBuffer.createBuffer(
      0, size: UInt(MemoryLayout<UInt8>.stride))
    super.init(values: values, nulls: nulls)
  }

  public func append(_ newValue: ItemType?) {
    let index = UInt(self.length)
    let byteIndex = self.stride * Int(index)
    self.length += 1
    if length > self.values.length {
      self.resize(length)
    }

    if let val = newValue {
      BitUtility.setBit(index + self.offset, buffer: self.nulls)
      self.values.rawPointer.advanced(by: byteIndex).storeBytes(
        of: val, as: T.self)
    } else {
      self.nullCount += 1
      BitUtility.clearBit(index + self.offset, buffer: self.nulls)
      self.values.rawPointer.advanced(by: byteIndex).storeBytes(
        of: defaultVal, as: T.self)
    }
  }

  public func resize(_ length: UInt) {
    if length > self.values.length {
      let resizeLength = resizeLength(self.values)
      var values = ArrowBuffer.createBuffer(
        resizeLength, size: UInt(MemoryLayout<T>.size))
      var nulls = ArrowBuffer.createBuffer(
        resizeLength / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
      ArrowBuffer.copyCurrent(
        self.values, to: &values, len: self.values.capacity)
      ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: self.nulls.capacity)
      self.values = values
      self.nulls = nulls
    }
  }

  public func finish() -> [ArrowBuffer] {
    let length = self.length
    var values = ArrowBuffer.createBuffer(
      length, size: UInt(MemoryLayout<T>.size))
    var nulls = ArrowBuffer.createBuffer(
      length / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
    ArrowBuffer.copyCurrent(self.values, to: &values, len: values.capacity)
    ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: nulls.capacity)
    return [nulls, values]
  }
}

public class BoolBufferBuilder: ValuesBufferBuilder<Bool>, ArrowBufferBuilder {
  public typealias ItemType = Bool
  public required init() throws(ArrowError) {
    let values = ArrowBuffer.createBuffer(
      0, size: UInt(MemoryLayout<UInt8>.stride))
    let nulls = ArrowBuffer.createBuffer(
      0, size: UInt(MemoryLayout<UInt8>.stride))
    super.init(values: values, nulls: nulls)
  }

  public func append(_ newValue: ItemType?) {
    let index = UInt(self.length)
    self.length += 1
    if (length / 8) > self.values.length {
      self.resize(length)
    }

    if newValue != nil {
      BitUtility.setBit(index + self.offset, buffer: self.nulls)
      if newValue == true {
        BitUtility.setBit(index + self.offset, buffer: self.values)
      } else {
        BitUtility.clearBit(index + self.offset, buffer: self.values)
      }

    } else {
      self.nullCount += 1
      BitUtility.clearBit(index + self.offset, buffer: self.nulls)
      BitUtility.clearBit(index + self.offset, buffer: self.values)
    }
  }

  public func resize(_ length: UInt) {
    if (length / 8) > self.values.length {
      let resizeLength = resizeLength(self.values)
      var values = ArrowBuffer.createBuffer(
        resizeLength, size: UInt(MemoryLayout<UInt8>.size))
      var nulls = ArrowBuffer.createBuffer(
        resizeLength, size: UInt(MemoryLayout<UInt8>.size))
      ArrowBuffer.copyCurrent(
        self.values, to: &values, len: self.values.capacity)
      ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: self.nulls.capacity)
      self.values = values
      self.nulls = nulls
    }
  }

  public func finish() -> [ArrowBuffer] {
    let length = self.length
    var values = ArrowBuffer.createBuffer(
      length, size: UInt(MemoryLayout<UInt8>.size))
    var nulls = ArrowBuffer.createBuffer(
      length, size: UInt(MemoryLayout<UInt8>.size))
    ArrowBuffer.copyCurrent(self.values, to: &values, len: values.capacity)
    ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: nulls.capacity)
    return [nulls, values]
  }
}

public class VariableBufferBuilder<T>: ValuesBufferBuilder<T>,
  ArrowBufferBuilder
where T: ByteConvertible {
  public typealias ItemType = T
  var offsets: ArrowBuffer
  let binaryStride = MemoryLayout<UInt8>.stride

  public required init() throws(ArrowError) {
    let values = ArrowBuffer.createBuffer(0, size: UInt(binaryStride))
    let nulls = ArrowBuffer.createBuffer(0, size: UInt(binaryStride))
    self.offsets = ArrowBuffer.createBuffer(
      0, size: UInt(MemoryLayout<Int32>.stride))
    super.init(values: values, nulls: nulls, stride: binaryStride)
  }

  public func append(_ newValue: ItemType?) {
    let index = UInt(self.length)
    self.length += 1
    let offsetIndex = MemoryLayout<Int32>.stride * Int(index)
    if self.length >= self.offsets.length {
      self.resize(UInt(self.offsets.length + 1))
    }
    var data: Data
    var isNull = false
    if let newValue {
      data = newValue.toData()
    } else {
      var nullVal = 0
      isNull = true
      data = Data(bytes: &nullVal, count: MemoryLayout<UInt32>.size)
    }
    var currentIndex: Int32 = 0
    var currentOffset: Int32 = Int32(data.count)
    if index > 0 {
      currentIndex = self.offsets.rawPointer.advanced(by: offsetIndex).load(
        as: Int32.self)
      currentOffset += currentIndex
      if currentOffset > self.values.length {
        self.valueResize(UInt(currentOffset))
      }
    }
    if isNull {
      self.nullCount += 1
      BitUtility.clearBit(index + self.offset, buffer: self.nulls)
    } else {
      BitUtility.setBit(index + self.offset, buffer: self.nulls)
    }
    data.withUnsafeBytes { buffer in
      UnsafeMutableRawBufferPointer(
        start: self.values.rawPointer.advanced(by: Int(currentIndex)),
        count: data.count
      ).copyBytes(from: buffer)
    }
    self.offsets.rawPointer.advanced(
      by: offsetIndex + MemoryLayout<Int32>.stride
    )
    .storeBytes(of: currentOffset, as: Int32.self)
  }

  public func valueResize(_ length: UInt) {
    if length > self.values.length {
      let resizeLength = resizeLength(self.values, len: length)
      var values = ArrowBuffer.createBuffer(
        resizeLength, size: UInt(MemoryLayout<UInt8>.size))
      ArrowBuffer.copyCurrent(
        self.values, to: &values, len: self.values.capacity)
      self.values = values
    }
  }

  public func resize(_ length: UInt) {
    if length > self.offsets.length {
      let resizeLength = resizeLength(self.offsets, len: length)
      var nulls = ArrowBuffer.createBuffer(
        resizeLength / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
      var offsets = ArrowBuffer.createBuffer(
        resizeLength, size: UInt(MemoryLayout<Int32>.size))
      ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: self.nulls.capacity)
      ArrowBuffer.copyCurrent(
        self.offsets, to: &offsets, len: self.offsets.capacity)
      self.nulls = nulls
      self.offsets = offsets
    }
  }

  public func finish() -> [ArrowBuffer] {
    let length = self.length
    var values = ArrowBuffer.createBuffer(
      self.values.length, size: UInt(MemoryLayout<UInt8>.size))
    var nulls = ArrowBuffer.createBuffer(
      length / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
    var offsets = ArrowBuffer.createBuffer(
      length, size: UInt(MemoryLayout<Int32>.size))
    ArrowBuffer.copyCurrent(self.values, to: &values, len: values.capacity)
    ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: nulls.capacity)
    ArrowBuffer.copyCurrent(self.offsets, to: &offsets, len: offsets.capacity)
    return [nulls, offsets, values]
  }
}

public class AbstractWrapperBufferBuilder<T, U>: ArrowBufferBuilder
where U: Numeric {
  public typealias ItemType = T
  public var capacity: UInt { self.bufferBuilder.capacity }
  public var length: UInt { self.bufferBuilder.length }
  public var nullCount: UInt { self.bufferBuilder.nullCount }
  public var offset: UInt { self.bufferBuilder.offset }
  let bufferBuilder: FixedBufferBuilder<U>
  public required init() throws(ArrowError) {
    self.bufferBuilder = try FixedBufferBuilder()
  }

  public func append(_ newValue: ItemType?) {
    fatalError("Method is not implemented")
  }

  public func isNull(_ index: UInt) -> Bool {
    self.bufferBuilder.isNull(index)
  }

  public func resize(_ length: UInt) {
    self.bufferBuilder.resize(length)
  }

  public func finish() -> [ArrowBuffer] {
    self.bufferBuilder.finish()
  }
}

public class Date32BufferBuilder: AbstractWrapperBufferBuilder<Date, Int32> {
  public override func append(_ newValue: ItemType?) {
    if let val = newValue {
      let daysSinceEpoch = Int32(val.timeIntervalSince1970 / 86400)
      self.bufferBuilder.append(daysSinceEpoch)
    } else {
      self.bufferBuilder.append(nil)
    }
  }
}

public class Date64BufferBuilder: AbstractWrapperBufferBuilder<Date, Int64> {
  public override func append(_ newValue: ItemType?) {
    if let val = newValue {
      let daysSinceEpoch = Int64(val.timeIntervalSince1970 * 1000)
      self.bufferBuilder.append(daysSinceEpoch)
    } else {
      self.bufferBuilder.append(nil)
    }
  }
}

public final class StructBufferBuilder: BaseBufferBuilder, ArrowBufferBuilder {
  public typealias ItemType = [Any?]
  var info: ArrowType?
  public init() throws(ArrowError) {
    let nulls = ArrowBuffer.createBuffer(
      0, size: UInt(MemoryLayout<UInt8>.stride))
    super.init(nulls)
  }

  public func initializeTypeInfo(_ fields: [ArrowField]) {
    info = .strct(fields)
  }

  public func append(_ newValue: [Any?]?) {
    let index = UInt(self.length)
    self.length += 1
    if self.length > self.nulls.length {
      self.resize(length)
    }

    if newValue != nil {
      BitUtility.setBit(index + self.offset, buffer: self.nulls)
    } else {
      self.nullCount += 1
      BitUtility.clearBit(index + self.offset, buffer: self.nulls)
    }
  }

  public func resize(_ length: UInt) {
    if length > self.nulls.length {
      let resizeLength = resizeLength(self.nulls)
      var nulls = ArrowBuffer.createBuffer(
        resizeLength / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
      ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: self.nulls.capacity)
      self.nulls = nulls
    }
  }

  public func finish() -> [ArrowBuffer] {
    let length = self.length
    var nulls = ArrowBuffer.createBuffer(
      length / 8 + 1,
      size: UInt(MemoryLayout<UInt8>.size)
    )
    ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: nulls.capacity)
    return [nulls]
  }
}

public class ListBufferBuilder: BaseBufferBuilder, ArrowBufferBuilder {
  public typealias ItemType = [Any?]
  var offsets: ArrowBuffer

  public required init() throws(ArrowError) {
    self.offsets = ArrowBuffer.createBuffer(
      1,
      size: UInt(MemoryLayout<Int32>.stride)
    )
    let nulls = ArrowBuffer.createBuffer(
      0,
      size: UInt(MemoryLayout<UInt8>.stride)
    )
    super.init(nulls)
    self.offsets.rawPointer.storeBytes(of: Int32(0), as: Int32.self)
  }

  public func append(_ count: Int) {
    let index = UInt(self.length)
    self.length += 1

    if length >= self.offsets.length {
      self.resize(length + 1)
    }

    let offsetIndex = Int(index) * MemoryLayout<Int32>.stride
    let currentOffset = self.offsets.rawPointer.advanced(by: offsetIndex).load(
      as: Int32.self)

    BitUtility.setBit(index + self.offset, buffer: self.nulls)
    let newOffset = currentOffset + Int32(count)
    self.offsets.rawPointer.advanced(
      by: offsetIndex + MemoryLayout<Int32>.stride
    ).storeBytes(
      of: newOffset, as: Int32.self)
  }

  public func append(_ newValue: [Any?]?) {
    let index = UInt(self.length)
    self.length += 1

    if self.length >= self.offsets.length {
      self.resize(self.length + 1)
    }

    let offsetIndex = Int(index) * MemoryLayout<Int32>.stride
    let currentOffset = self.offsets.rawPointer.advanced(by: offsetIndex).load(
      as: Int32.self)

    if let vals = newValue {
      BitUtility.setBit(index + self.offset, buffer: self.nulls)
      let newOffset = currentOffset + Int32(vals.count)
      self.offsets.rawPointer.advanced(
        by: offsetIndex + MemoryLayout<Int32>.stride
      ).storeBytes(
        of: newOffset, as: Int32.self)
    } else {
      self.nullCount += 1
      BitUtility.clearBit(index + self.offset, buffer: self.nulls)
      self.offsets.rawPointer.advanced(
        by: offsetIndex + MemoryLayout<Int32>.stride
      ).storeBytes(
        of: currentOffset, as: Int32.self)
    }
  }

  public override func isNull(_ index: UInt) -> Bool {
    !BitUtility.isSet(index + self.offset, buffer: self.nulls)
  }

  public func resize(_ length: UInt) {
    if length > self.offsets.length {
      let resizeLength = resizeLength(self.offsets)
      var offsets = ArrowBuffer.createBuffer(
        resizeLength, size: UInt(MemoryLayout<Int32>.size))
      var nulls = ArrowBuffer.createBuffer(
        resizeLength / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
      ArrowBuffer.copyCurrent(
        self.offsets, to: &offsets, len: self.offsets.capacity)
      ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: self.nulls.capacity)
      self.offsets = offsets
      self.nulls = nulls
    }
  }

  public func finish() -> [ArrowBuffer] {
    let length = self.length
    var nulls = ArrowBuffer.createBuffer(
      length / 8 + 1, size: UInt(MemoryLayout<UInt8>.size))
    var offsets = ArrowBuffer.createBuffer(
      length + 1, size: UInt(MemoryLayout<Int32>.size))
    ArrowBuffer.copyCurrent(self.nulls, to: &nulls, len: nulls.capacity)
    ArrowBuffer.copyCurrent(self.offsets, to: &offsets, len: offsets.capacity)
    return [nulls, offsets]
  }
}
