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

/// An Arrow buffer.
public protocol ArrowBufferProtocol {
  func withUnsafeBytes<R>(
    _ body: (UnsafeRawBufferPointer) throws -> R
  ) rethrows -> R
}

internal protocol ArrowBufferUInt8: ArrowBufferProtocol {
  var length: Int { get }
  var buffer: UnsafePointer<UInt8> { get }
}

extension ArrowBufferUInt8 {

  public func withUnsafeBytes<R>(
    _ body: (UnsafeRawBufferPointer) throws -> R
  ) rethrows -> R {
    let rawPointer = UnsafeRawPointer(buffer)
    let rawBuffer = UnsafeRawBufferPointer(start: rawPointer, count: length)
    return try body(rawBuffer)
  }
}

/// An empty Arrow buffer.
public protocol ArrowBufferEmpty: ArrowBufferProtocol {}

extension ArrowBufferEmpty {
  public func withUnsafeBytes<R>(
    _ body: (UnsafeRawBufferPointer) throws -> R
  ) rethrows -> R {
    let buffer = UnsafeRawBufferPointer(start: nil, count: 0)
    return try body(buffer)
  }
}
