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

/// A builder for bit-packed buffers used to represent nulls and booleans in Arrow arrays.
final class NullBufferBuilder {
  var length: Int
  var capacity: Int
  var bitCount: Int = 0
  var valueCount: Int = 0
  var nullCount: Int = 0
  private var buffer: UnsafeMutablePointer<UInt8>
  private var ownsMemory: Bool
  private var currentByte: UInt8 = 0
  private var bitOffset: Int8 = 0

  init(
    length: Int = 0,
    minCapacity: Int = 64
  ) {
    self.length = length
    self.capacity = minCapacity
    // Currently unaligned: probably doesn't need to be for a builder.
    self.buffer = .allocate(capacity: capacity)
    self.ownsMemory = true
  }

  /// Appends a validity bit to the buffer.
  @inline(__always)
  func appendValid(_ isValid: Bool) {
    valueCount += 1
    if isValid {
      currentByte |= 1 << bitOffset
    } else {
      nullCount += 1
    }
    bitOffset += 1
    bitCount += 1
    if bitOffset == 8 {
      flushByte()
    }
  }

  @inline(__always)
  private func flushByte() {
    // ensure we have space to write at index `length`
    if length >= capacity {
      resize(to: capacity * 2)
    }
    buffer[length] = currentByte
    currentByte = 0
    bitOffset = 0
    length += 1
  }

  private func resize(to newCapacity: Int) {
    precondition(newCapacity > capacity)
    let newBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: newCapacity)
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

  /// Builds completed `NullBuffer` with 64-byte alignment, flushing any intermediate state.
  ///
  /// Memory ownership is transferred to the returned `NullBuffer`. Any memory held is deallocated.
  /// - Returns: The completed `NullBuffer` which in the case of zero nulls will be a struct
  /// which always reports validity to be true, or if all nulls the inverse of this. Otherwise a buffer  with
  /// capacity shrunk to a multiple of 64 bytes will be returned.
  ///
  func finish() -> NullBuffer {
    if bitOffset != 0 {
      flushByte()
    }
    precondition(ownsMemory, "Buffer already finished.")

    if nullCount == 0 {
      return AllValidNullBuffer(valueCount: valueCount)
    } else if nullCount == valueCount {
      return AllNullBuffer(valueCount: valueCount)
    } else {
      ownsMemory = false
      let newCapacity = (length + 63) & ~63
      let newBuffer = UnsafeMutableRawPointer.allocate(
        byteCount: newCapacity,
        alignment: 64
      ).bindMemory(to: UInt8.self, capacity: newCapacity)
      newBuffer.initialize(from: buffer, count: length)
      buffer.deallocate()
      return BitPackedNullBuffer(
        length: length,
        capacity: newCapacity,
        valueCount: valueCount,
        nullCount: nullCount,
        ownsMemory: true,
        buffer: newBuffer
      )
    }
  }
}
