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
  var valueCount: Int
  var valueCapacity: Int
  private var buffer: UnsafeMutablePointer<T>
  private var ownsMemory: Bool
  private var bitOffset: Int8 = 0

  init(
    minCapacity: Int = 1024
  ) {
    self.valueCount = 0
    // Ensure at least 1 element capacity
    self.valueCapacity = max(1, minCapacity / MemoryLayout<T>.size)
    self.buffer = .allocate(capacity: valueCapacity)
    self.ownsMemory = true
  }

  func append(_ val: T) {
    if valueCount >= valueCapacity {
      var newCapacity = valueCapacity * 2
      while valueCount >= newCapacity {
        newCapacity *= 2
      }
      resize(to: newCapacity)
    }
    buffer[valueCount] = val
    valueCount += 1
  }

  private func resize(to newCapacity: Int) {
    precondition(newCapacity > valueCapacity)
    let newBuffer = UnsafeMutablePointer<T>.allocate(capacity: newCapacity)
    newBuffer.initialize(from: buffer, count: valueCount)
    buffer.deallocate()
    buffer = newBuffer
    valueCapacity = newCapacity
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
    let byteCount = valueCount * MemoryLayout<T>.size
    let newCapacity = (byteCount + 63) & ~63
    let newBuffer = UnsafeMutableRawPointer.allocate(
      byteCount: newCapacity,
      alignment: 64
    ).bindMemory(to: T.self, capacity: newCapacity)
    newBuffer.initialize(from: buffer, count: valueCount)
    buffer.deallocate()
    return FixedWidthBuffer(
      length: byteCount,
      capacity: newCapacity,
      valueCount: valueCount,
      ownsMemory: true,
      buffer: newBuffer
    )
  }
}
