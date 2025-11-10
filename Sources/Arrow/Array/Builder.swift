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

  public func append(_ val: Bool?) {
    length += 1
    if let val {
      nullBuilder.appendValid(true)
      valueBuilder.appendValid(val)
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

  public func append(_ val: T?) {
    length += 1
    if let val {
      nullBuilder.appendValid(true)
      valueBuilder.append(val)
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

  public func append(_ val: T?) {
    length += 1
    if let val {
      nullBuilder.appendValid(true)
      let data = val.data
      if valueBuilder.length + data.count > valueBuilder.capacity {
        valueBuilder.doubleCapacity()
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
