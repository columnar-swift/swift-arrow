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

/// An Arrow type with variable length.
protocol VariableLength {
  init(_ value: UnsafeBufferPointer<UInt8>)

  var data: Data { get }
}

extension String: VariableLength {
  init(_ value: UnsafeBufferPointer<UInt8>) {
    self.init(decoding: value, as: Unicode.UTF8.self)
  }

  var data: Data {
    Data(self.utf8)
  }
}

extension Data: VariableLength {
  init(value: UnsafeBufferPointer<UInt8>) {
    self.init(value)
  }

  var data: Data {
    self
  }
}

/// A buffer containing values with variable length, used in variable length type Arrow arrays.
final class VariableLengthTypeBuffer<T: VariableLength> {
  var length: Int
  var capacity: Int
  let ownsMemory: Bool
  var buffer: UnsafePointer<UInt8>

  init(
    length: Int,
    capacity: Int,
    ownsMemory: Bool,
    buffer: UnsafePointer<UInt8>
  ) {
    self.length = length
    self.capacity = capacity
    self.ownsMemory = ownsMemory
    self.buffer = buffer
  }

  func loadVariable(
    at startIndex: Int,
    arrayLength: Int
  ) -> T {
    precondition(startIndex + arrayLength <= self.length)
    let rawPointer = buffer.advanced(by: startIndex)
    let buffer = UnsafeBufferPointer<UInt8>(
      start: rawPointer,
      count: arrayLength
    )
    return T(buffer)
  }

  deinit {
    if ownsMemory {
      buffer.deallocate()
    }
  }
}
