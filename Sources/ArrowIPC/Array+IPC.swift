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
import Foundation

typealias ArrowArrayUtf8 = ArrowArrayVariable<
  String,
  FixedWidthBufferIPC<Int32>,
  VariableLengthBufferIPC<String>
>

extension ArrowArrayUtf8 {

  /// Build a `Data` backed Arrow utf8 array.
  /// - Parameters:
  ///   - length: The array length.
  ///   - nullBuffer: The null buffer.
  ///   - offsetsBuffer: A view over file-backed data.
  ///   - valueBuffer: A view over file-backed data.
  /// - Returns: A file-backed Arrow utf8 array.
  static func utf8(
    length: Int,
    nullBuffer: NullBuffer,
    offsetsBuffer: FileDataBuffer,
    valueBuffer: FileDataBuffer
  ) -> Self {
    let offsetsBufferTyped = FixedWidthBufferIPC<Int32>(buffer: offsetsBuffer)
    let valueBufferTyped = VariableLengthBufferIPC<String>(buffer: valueBuffer)
    return Self(
      offset: 0,
      length: length,
      nullBuffer: nullBuffer,
      offsetsBuffer: offsetsBufferTyped,
      valueBuffer: valueBufferTyped
    )
  }
}

typealias ArrowArrayBinary = ArrowArrayVariable<
  Data,
  FixedWidthBufferIPC<Int32>,
  VariableLengthBufferIPC<Data>
>

extension ArrowArrayBinary {

  /// Build a `Data` backed Arrow binary array.
  /// - Parameters:
  ///   - length: The array length.
  ///   - nullBuffer: The null buffer.
  ///   - offsetsBuffer: A view over file-backed data.
  ///   - valueBuffer: A view over file-backed data.
  /// - Returns: A file-backed Arrow utf8 array.
  static func binary(
    length: Int,
    nullBuffer: NullBuffer,
    offsetsBuffer: FileDataBuffer,
    valueBuffer: FileDataBuffer
  ) -> Self {
    let offsetsBufferTyped = FixedWidthBufferIPC<Int32>(buffer: offsetsBuffer)
    let valueBufferTyped = VariableLengthBufferIPC<Data>(buffer: valueBuffer)
    return Self(
      offset: 0,
      length: length,
      nullBuffer: nullBuffer,
      offsetsBuffer: offsetsBufferTyped,
      valueBuffer: valueBufferTyped
    )
  }
}
