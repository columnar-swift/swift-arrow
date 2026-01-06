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

public protocol FixedWidthBufferProtocol<ElementType>: ArrowBufferProtocol {
  associatedtype ElementType
  var length: Int { get }
  subscript(index: Int) -> ElementType { get }
}

/// A  buffer used in Arrow arrays that hold fixed-width types.
public final class FixedWidthBuffer<T>: @unchecked Sendable,
  FixedWidthBufferProtocol
{
  public typealias ElementType = T
  public let length: Int
  let capacity: Int
  let valueCount: Int
  let ownsMemory: Bool
  let buffer: UnsafePointer<T>

  public init(
    length: Int,
    capacity: Int,
    valueCount: Int,
    ownsMemory: Bool,
    buffer: UnsafePointer<T>
  ) {
    self.length = length
    self.capacity = capacity
    self.valueCount = valueCount
    self.ownsMemory = ownsMemory
    self.buffer = buffer
  }

  public subscript(index: Int) -> T {
    buffer[index]
  }

  public func withUnsafeBytes<R>(
    _ body: (UnsafeRawBufferPointer) throws -> R
  ) rethrows -> R {
    let rawPointer = UnsafeRawPointer(buffer)
    let byteCount = valueCount * MemoryLayout<T>.stride
    let buffer = UnsafeRawBufferPointer(start: rawPointer, count: byteCount)
    return try body(buffer)
  }

  deinit {
    if ownsMemory {
      buffer.deallocate()
    }
  }
}

extension FixedWidthBuffer {

  /// Build a fixed-width buffer from a fixed-width type array.
  /// - Parameter values: The array to opy memory from.
  /// - Returns: A buffer with the values copied into..
  public static func from(_ values: [T]) -> FixedWidthBuffer<T> {
    let count = values.count
    let capacity = count * MemoryLayout<T>.stride
    let buffer = UnsafeMutablePointer<T>.allocate(capacity: count)
    // Copy values
    for (index, value) in values.enumerated() {
      buffer[index] = value
    }
    return FixedWidthBuffer(
      length: capacity,
      capacity: capacity,
      valueCount: count,
      ownsMemory: true,
      buffer: UnsafePointer(buffer)
    )
  }
}
