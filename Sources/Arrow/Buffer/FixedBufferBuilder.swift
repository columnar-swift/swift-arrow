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

final class FixedWidthBufferBuilder<T: Numeric> {
  var length: Int
  var capacity: Int
  private var buffer: UnsafeMutablePointer<T>
  private var ownsMemory: Bool
  private var bitOffset: Int8 = 0

  init(
    minCapacity: Int = 4096
  ) {
    self.length = 0
    self.capacity = minCapacity / MemoryLayout<T>.size
    self.buffer = .allocate(capacity: capacity)
    self.ownsMemory = true
  }

  func append(_ val: T) {
    if length >= capacity {
      resize(to: capacity * 2)
    }
    buffer[length] = val
    length += 1
  }

  private func resize(to newCapacity: Int) {
    precondition(newCapacity > capacity)
    let newBuffer = UnsafeMutablePointer<T>.allocate(capacity: newCapacity)
    newBuffer.initialize(from: buffer, count: length)
    buffer.deallocate()
    buffer = newBuffer
    capacity = newCapacity
  }

  deinit {
    if ownsMemory {
      buffer.deallocate()
    }
  }

  /// Builds completed `FixedWidthBuffer` with 64-byte alignment.
  ///
  /// Memory ownership is transferred to the returned `FixedWidthBuffer`. Any memory held is
  /// deallocated.
  /// - Returns: the completed `FixedWidthBuffer` with capacity shrunk to a multiple of 64 bytes.
  func finish() -> FixedWidthBuffer<T> {
    precondition(ownsMemory, "Buffer already finished.")
    ownsMemory = false
    let byteCount = length * MemoryLayout<T>.size
    let newCapacity = (byteCount + 63) & ~63
    let newBuffer = UnsafeMutableRawPointer.allocate(
      byteCount: newCapacity,
      alignment: 64
    ).bindMemory(to: T.self, capacity: newCapacity)
    newBuffer.initialize(from: buffer, count: length)
    buffer.deallocate()
    return FixedWidthBuffer(
      length: length,
      capacity: newCapacity,
      ownsMemory: true,
      buffer: newBuffer
    )
  }
}
