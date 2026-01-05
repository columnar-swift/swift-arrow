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

/// A 16-byte view that either stores data inline or references a buffer.
@frozen public struct BinaryView {
  private var words: InlineArray<4, Int32>

  private static let inlineThreshold = 12

  public init() {
    self.words = InlineArray(repeating: 0)
  }

  public var length: Int32 { words[0] }

  private mutating func setLength(_ value: Int32) {
    words[0] = value
  }

  public var isInline: Bool {
    length <= Self.inlineThreshold
  }

  /// Access inline data for values ≤12 bytes.
  public func withInlineData<R>(
    _ body: (Span<UInt8>) throws -> R
  ) rethrows -> R {
    precondition(isInline, "View is not inline.")
    return try words.span.withUnsafeBytes { buffer in
      // Skip first 4 bytes (length), read next `length` bytes
      let dataBuffer = UnsafeRawBufferPointer(
        start: buffer.baseAddress?.advanced(by: 4),
        count: Int(length)
      )
      let dataSpan = Span(
        _unsafeElements: dataBuffer.bindMemory(to: UInt8.self)
      )
      return try body(dataSpan)
    }
  }

  // MARK: - Referenced data access

  public var prefix: UInt32 {
    precondition(!isInline, "View is inline.")
    return UInt32(bitPattern: words.span[1])
  }

  public var bufferIndex: Int32 {
    precondition(!isInline, "View is inline.")
    return words.span[2]
  }

  public var offset: Int32 {
    precondition(!isInline, "View is inline.")
    return words.span[3]
  }

  // MARK: - Creation

  /// Create an inline view (for length ≤ 12).
  public static func inline(_ data: Span<UInt8>) -> BinaryView {
    precondition(
      data.count <= inlineThreshold,
      "Data too large for inline storage."
    )
    var view = BinaryView()
    view.setLength(Int32(data.count))
    var mutableSpan = view.words.mutableSpan
    mutableSpan.withUnsafeMutableBytes { buffer in
      // Copy data starting at byte 4
      data.withUnsafeBytes { sourceBytes in
        let dest = UnsafeMutableRawBufferPointer(
          start: buffer.baseAddress?.advanced(by: 4),
          count: data.count
        )
        dest.copyMemory(from: sourceBytes)
      }
    }
    return view
  }

  /// Create a referenced view (for length > 12) - trivial!
  public static func referenced(
    length: Int32,
    prefix: UInt32,
    bufferIndex: Int32,
    offset: Int32
  ) -> BinaryView {
    precondition(
      length > inlineThreshold,
      "Data small enough for inline storage."
    )
    var view = BinaryView()
    view.words[0] = length
    view.words[1] = Int32(bitPattern: prefix)
    view.words[2] = bufferIndex
    view.words[3] = offset

    return view
  }
}
