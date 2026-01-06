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

import Arrow

/// An Arrow buffer backed by file data.
internal protocol ArrowBufferIPC: ArrowBufferProtocol {
  var buffer: FileDataBuffer { get }
}

extension ArrowBufferIPC {
  public func withUnsafeBytes<R>(
    _ body: (UnsafeRawBufferPointer) throws -> R
  ) rethrows -> R {
    try buffer.data.withUnsafeBytes { dataPtr in
      let rangedPtr = UnsafeRawBufferPointer(
        rebasing: dataPtr[buffer.range]
      )
      return try body(rangedPtr)
    }
  }
}

/// A `Data` backed buffer for null bitmaps and boolean arrays.
struct NullBufferIPC: NullBuffer, ArrowBufferIPC {

  let buffer: FileDataBuffer
  var valueCount: Int
  let nullCount: Int

  var length: Int { (valueCount + 7) / 8 }

  func isSet(_ bit: Int) -> Bool {
    precondition(bit < valueCount, "Bit index \(bit) out of range")
    let byteIndex = bit / 8
    //    precondition(length > byteIndex, "Bit index \(bit) out of range")
    let offsetIndex = buffer.range.lowerBound + byteIndex
    let byte = self.buffer.data[offsetIndex]
    return byte & (1 << (bit % 8)) > 0
  }
}

/// A `Data` backed buffer for fixed-width types.
struct FixedWidthBufferIPC<Element>: FixedWidthBufferProtocol, ArrowBufferIPC
where Element: BitwiseCopyable {
  typealias ElementType = Element
  let buffer: FileDataBuffer
  var length: Int { buffer.range.count }

  init(buffer: FileDataBuffer) {
    self.buffer = buffer
  }

  subscript(index: Int) -> Element {
    buffer.data.withUnsafeBytes { rawBuffer in
      let sub = rawBuffer[buffer.range]
      let span = Span<Element>(_unsafeBytes: sub)
      return span[index]
    }
  }
}

/// A `Data` backed buffer for variable-length types.
struct VariableLengthBufferIPC<
  Element: VariableLength, OffsetType: FixedWidthInteger
>:
  VariableLengthBufferProtocol, ArrowBufferIPC
{
  typealias ElementType = Element
  let buffer: FileDataBuffer
  var length: Int { buffer.range.count }
  init(buffer: FileDataBuffer) {
    self.buffer = buffer
  }

  func loadVariable(
    at startIndex: Int,
    arrayLength: Int
  ) -> Element {
    precondition(startIndex + arrayLength <= self.length)
    return buffer.data.withUnsafeBytes { rawBuffer in
      let offsetStart = buffer.range.lowerBound + startIndex
      let offsetEnd = offsetStart + arrayLength
      let slice = rawBuffer[offsetStart..<offsetEnd]
      let uint8Buffer = slice.bindMemory(to: UInt8.self)
      return Element(uint8Buffer)
    }
  }
}
