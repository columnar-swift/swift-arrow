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

protocol ArrowArrayProtocol {
  associatedtype ItemType
  subscript(_ index: Int) -> ItemType? { get }
}

/// An Arrow array of booleans using the three-valued logical model (true / false / null).
struct ArrowArrayBoolean: ArrowArrayProtocol {
  typealias ItemType = Bool
  let length: Int
  let nullBuffer: NullBuffer
  let valueBuffer: NullBuffer

  subscript(index: Int) -> Bool? {
    if !self.nullBuffer.isSet(index) {
      return nil
    }
    return valueBuffer.isSet(index)
  }
}

/// An Arrow array of fixed-width types.
struct ArrowArrayFixed<T>: ArrowArrayProtocol where T: Numeric {
  typealias ItemType = T
  let length: Int
  let nullBuffer: NullBuffer
  let valueBuffer: FixedWidthBuffer<T>

  subscript(index: Int) -> T? {
    if !self.nullBuffer.isSet(index) {
      return nil
    }
    return valueBuffer[index]
  }
}

/// An Arrow array of variable-length types.
struct ArrowArrayVariable<T>: ArrowArrayProtocol where T: VariableLength {
  typealias ItemType = T
  let length: Int
  let nullBuffer: NullBuffer
  let offsetsBuffer: FixedWidthBuffer<UInt32>
  let valueBuffer: VariableLengthTypeBuffer<T>

  subscript(index: Int) -> T? {
    if !self.nullBuffer.isSet(index) {
      return nil
    }
    let startIndex = offsetsBuffer[index]
    let endIndex = offsetsBuffer[index + 1]
    return valueBuffer.loadVariable(
      at: Int(startIndex),
      arrayLength: Int(endIndex - startIndex)
    )
  }
}

/// An Arrow array of `Date`s with a resolution of 1 day.
struct ArrowArrayDate32: ArrowArrayProtocol {
  typealias ItemType = Date

  let array: ArrowArrayFixed<Date32>

  var length: Int {
    array.length
  }

  subscript(index: Int) -> Date? {
    let days: Int32? = array[index]
    if let days {
      return Date(timeIntervalSince1970: TimeInterval(days * 86400))
    } else {
      return nil
    }
  }
}

/// An Arrow array of `Date`s with a resolution of 1 second.
struct ArrowArrayDate64: ArrowArrayProtocol {
  typealias ItemType = Date

  let array: ArrowArrayFixed<Date64>

  var length: Int {
    array.length
  }

  subscript(index: Int) -> Date? {
    let milliseconds: Int64? = array[index]
    if let milliseconds {
      return Date(timeIntervalSince1970: TimeInterval(milliseconds / 1000))
    } else {
      return nil
    }
  }
}
