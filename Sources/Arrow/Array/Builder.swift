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

/// A builder for Arrow arrays using the three-valued logical model (true / false / null).
class ArrayBuilderBoolean {

  var length: Int
  let nullBuilder: NullBufferBuilder
  let valueBuilder: NullBufferBuilder

  init() {
    self.length = 0
    self.nullBuilder = NullBufferBuilder()
    self.valueBuilder = NullBufferBuilder()
  }

  public func append(_ value: Bool?) {
    length += 1
    if let value {
      nullBuilder.appendValid(true)
      valueBuilder.appendValid(value)
    } else {
      nullBuilder.appendValid(false)
    }
  }

  public func finish() -> ArrowArrayBoolean {
    let nullBuffer = nullBuilder.finish()
    let valueBuffer = valueBuilder.finish()

    return ArrowArrayBoolean(
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// A builder for Arrow arrays holding fixed-width types.
class ArrayBuilderFixedWidth<T: Numeric> {

  var length: Int
  let nullBuilder: NullBufferBuilder
  let valueBuilder: FixedWidthBufferBuilder<T>

  init() {
    self.length = 0
    self.nullBuilder = NullBufferBuilder()
    self.valueBuilder = FixedWidthBufferBuilder<T>()
  }

  public func append(_ value: T?) {
    length += 1
    if let value {
      nullBuilder.appendValid(true)
      valueBuilder.append(value)
    } else {
      nullBuilder.appendValid(false)
      valueBuilder.append(T.zero)
    }
  }

  public func finish() -> ArrowArrayFixed<T> {
    let nullBuffer = nullBuilder.finish()
    let valueBuffer = valueBuilder.finish()

    return ArrowArrayFixed(
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// A builder for Arrow arrays holding variable length types.
class ArrayBuilderVariable<T: VariableLength> {
  var length: Int
  let nullBuilder: NullBufferBuilder
  let offsetsBuilder: FixedWidthBufferBuilder<UInt32>
  let valueBuilder: VariableLengthTypeBufferBuilder<T>

  init() {
    self.length = 0
    self.nullBuilder = NullBufferBuilder()
    self.offsetsBuilder = FixedWidthBufferBuilder<UInt32>()
    self.valueBuilder = VariableLengthTypeBufferBuilder<T>()
    self.offsetsBuilder.append(UInt32.zero)
  }

  public func append(_ value: T?) {
    length += 1
    if let value {
      nullBuilder.appendValid(true)
      let data = value.data
      let requiredCapacity = valueBuilder.length + data.count
      if requiredCapacity > valueBuilder.capacity {
        var newCapacity = valueBuilder.capacity
        while newCapacity < requiredCapacity {
          newCapacity *= 2
        }
        valueBuilder.increaseCapacity(to: newCapacity)
      }
      valueBuilder.append(data)
      let newOffset = UInt32(valueBuilder.length)
      offsetsBuilder.append(newOffset)
    } else {
      nullBuilder.appendValid(false)
      let newOffset = UInt32(valueBuilder.length)
      offsetsBuilder.append(newOffset)
    }
  }

  public func finish() -> ArrowArrayVariable<T> {
    let nullBuffer = nullBuilder.finish()
    let offsetsBuffer = offsetsBuilder.finish()
    let valueBuffer = valueBuilder.finish()
    return ArrowArrayVariable(
      length: length,
      nullBuffer: nullBuffer,
      offsetsBuffer: offsetsBuffer,
      valueBuffer: valueBuffer
    )
  }
}

/// A builder for Arrow arrays holding `Date`s with a resolution of one day.
struct ArrayBuilderDate32 {
  let builder: ArrayBuilderFixedWidth<Date32> = .init()

  public func append(_ value: Date?) {
    if let value {
      let daysSinceEpoch = Int32(value.timeIntervalSince1970 / 86400)
      self.builder.append(daysSinceEpoch)
    } else {
      self.builder.append(nil)
    }
  }

  func finish() -> ArrowArrayDate32 {
    .init(array: builder.finish())
  }
}

/// A builder for Arrow arrays holding `Date`s with a resolution of one day.
struct ArrayBuilderDate64 {
  let builder: ArrayBuilderFixedWidth<Date64> = .init()

  public func append(_ value: Date?) {
    if let value {
      let millisecondsSinceEpoch = Int64(value.timeIntervalSince1970 * 1000)
      self.builder.append(millisecondsSinceEpoch)
    } else {
      self.builder.append(nil)
    }
  }

  func finish() -> ArrowArrayDate64 {
    .init(array: builder.finish())
  }
}

typealias ArrayBuilderTime32 = ArrayBuilderFixedWidth<Time32>

typealias ArrayBuilderTime64 = ArrayBuilderFixedWidth<Time64>

typealias ArrayBuilderTimestamp = ArrayBuilderFixedWidth<Timestamp>
