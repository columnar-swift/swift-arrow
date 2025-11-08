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

protocol VariableLength {
  init (_ value: UnsafeBufferPointer<UInt8>)
}

extension String: VariableLength {
  init(_ value: UnsafeBufferPointer<UInt8>) {
    self.init(decoding: value, as: Unicode.UTF8.self)
  }
}

extension Data: VariableLength {
  init(value: UnsafeBufferPointer<UInt8>) {
    self.init(value)
  }
}

public struct ArrowData {
  
  // FIXME: Remove
  public var bufferData: [Data] {
    buffers.map { buffer in
      var data = Data()
      buffer.append(to: &data)
      return data
    }
  }

  // FIXME: Remove
  public var bufferDataSizes: [Int] {
    buffers.map { Int($0.capacity) }
  }
  
  // FIXME: Remove
  public var data: [UnsafeMutableRawPointer] {
    buffers.map { $0.rawPointer }
  }
  
  // FIXME: Remove
  public var bufferCount: Int {
    return buffers.count
  }
  
  // FIXME: Remove
  private let buffers: [ArrowBuffer]
  
  // TODO: Typed accessors - migration
  var offsets: OffsetsBuffer {
    if !type.isVariable && !type.isNested {
      fatalError()
    }
    return ArrowBufferBackedOffsets(buffers[1])
  }
  
  // TODO: this should replace nullBuffer
  var nulls: NullBuffer {
    let buffer = buffers[0]
    let pointer = buffer.rawPointer.assumingMemoryBound(to: UInt8.self)
    return NullBuffer(length: Int(buffer.length), capacity: 0, ownsMemory: false, buffer: pointer)
  }

  public let type: ArrowType
  public let children: [ArrowData]
  public let nullCount: UInt
  public let length: UInt

  let nullBuffer: ArrowBuffer

  init(
    _ arrowType: ArrowType,
    buffers: [ArrowBuffer],
    nullCount: UInt
  ) {
    self.init(
      arrowType, buffers: buffers,
      children: [ArrowData](),
      nullCount: nullCount,
      length: buffers[1].length
    )
  }

  init(
    _ arrowType: ArrowType,
    buffers: [ArrowBuffer],
    children: [ArrowData],
    nullCount: UInt,
    length: UInt
  ) {
    self.type = arrowType
    self.buffers = buffers
    self.children = children
    self.nullCount = nullCount
    self.length = length
    self.nullBuffer = buffers[0]
  }
  
  // TODO: Temporary while removing ArrowBuffer
  public func load<T>(at index: UInt) -> T where T: BitwiseCopyable {
    let valueType = T.self
    let byteOffset = type.getStride() * Int(index)
    let milliseconds = buffers[1].rawPointer.advanced(
      by: byteOffset
    ).load(as: valueType)
    return milliseconds
  }
  
  // TODO: Temporary while removing ArrowBuffer
  func loadVariable<T>(
    at startIndex: Int,
    arrayLength: Int
  ) -> T where T: VariableLength {
    let values = buffers[2]
    let rawPointer = values.rawPointer.advanced(by: startIndex)
      .bindMemory(to: UInt8.self, capacity: arrayLength)
    let buffer = UnsafeBufferPointer<UInt8>(
      start: rawPointer, count: arrayLength)
    return T(buffer)
  }

  // TODO: Temporary while removing ArrowBuffer
  public func isNull(_ at: UInt) -> Bool {
    let a = nulls.length > 0 && !nulls.isSet(Int(at))
    let b = nullBuffer.length > 0 && !BitUtility.isSet(at, buffer: nullBuffer)
    if nulls.length != nullBuffer.length {
      fatalError("Check new null handling")
    }
    if a != b {
      fatalError("Check new null handling")
    }
    return a
  }
  
  // TODO: Temporary while removing ArrowBuffer
  func isNullValue(at index: UInt) -> Bool {
    let valueBuffer = buffers[1]
    return BitUtility.isSet(index, buffer: valueBuffer)
  }
  
}
