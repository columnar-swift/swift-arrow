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

public protocol FixedWidthBufferProtocol<ElementType> {
  associatedtype ElementType: Numeric
  var length: Int { get }
  subscript(index: Int) -> ElementType { get }
}

public protocol Int32BufferProtocol {
  var length: Int { get }
  subscript(index: Int) -> Int32 { get }
}

/// A  buffer used in Arrow arrays that hold fixed-width types.
final class FixedWidthBuffer<T>: FixedWidthBufferProtocol where T: Numeric {

  typealias ElementType = T

  var length: Int
  var capacity: Int
  let valueCount: Int
  let ownsMemory: Bool
  var buffer: UnsafePointer<T>

  init(
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

  subscript(index: Int) -> T {

    buffer[index]
  }

  deinit {
    if ownsMemory {
      buffer.deallocate()
    }
  }
}
