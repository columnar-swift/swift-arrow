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

/// A builder for Arrow arrays holding binary view types.
public class ArrayBuilderBinaryView<Element: VariableLength>: AnyArrayBuilder {
  public typealias ArrayType = ArrowArrayBinaryView<Element>

  public var length: Int
  let nullBuilder: NullBufferBuilder
  let viewsBuilder: FixedWidthBufferBuilder<BinaryView>
  var dataBuffers: [VariableLengthTypeBufferBuilder<Element>]
  var currentDataBuffer: VariableLengthTypeBufferBuilder<Element>

  private let inlineThreshold = 12
  // Limit buffer size to avoid huge buffers
  private let maxBufferSize: Int

  public init(maxBufferSize: Int = 2 * 1024 * 1024) {
    self.length = 0
    self.nullBuilder = NullBufferBuilder()
    self.viewsBuilder = FixedWidthBufferBuilder<BinaryView>()
    self.dataBuffers = []
    self.currentDataBuffer = VariableLengthTypeBufferBuilder<Element>()
    self.maxBufferSize = maxBufferSize
  }

  public func append(_ value: Element) {
    length += 1
    nullBuilder.appendValid(true)

    let data = value.data

    if data.count <= self.inlineThreshold {
      // Store inline
      let view = BinaryView.inline(data.span)
      viewsBuilder.append(view)
    } else {
      // Check if we need a new buffer
      if currentDataBuffer.length + data.count > maxBufferSize {
        // Finalize current buffer and start a new one
        dataBuffers.append(currentDataBuffer)
        currentDataBuffer = VariableLengthTypeBufferBuilder<Element>()
      }

      // Ensure capacity
      let requiredCapacity = currentDataBuffer.length + data.count
      if requiredCapacity > currentDataBuffer.capacity {
        var newCapacity = currentDataBuffer.capacity
        while newCapacity < requiredCapacity {
          newCapacity *= 2
        }
        currentDataBuffer.increaseCapacity(to: newCapacity)
      }

      let offset = currentDataBuffer.length
      currentDataBuffer.append(data)

      // Extract prefix (first 4 bytes)
      let prefix: UInt32 = data.withUnsafeBytes { bytes in
        guard bytes.count >= 4 else { return 0 }
        return bytes.loadUnaligned(as: UInt32.self)
      }

      let view = BinaryView.referenced(
        length: Int32(data.count),
        prefix: prefix,
        bufferIndex: Int32(dataBuffers.count),  // Current buffer index
        offset: Int32(offset)
      )
      viewsBuilder.append(view)
    }
  }

  public func appendNull() {
    length += 1
    nullBuilder.appendValid(false)
    // Append a zero-length inline view for null
    let emptyView = BinaryView()
    viewsBuilder.append(emptyView)
  }

  public func finish() -> ArrayType {
    // Add current buffer to the list if it has data
    if currentDataBuffer.length > 0 {
      dataBuffers.append(currentDataBuffer)
    }

    // Finish all data buffers
    let finishedDataBuffers = dataBuffers.map { $0.finish() }

    return ArrayType(
      offset: 0,
      length: length,
      nullBuffer: nullBuilder.finish(),
      viewsBuffer: viewsBuilder.finish(),
      dataBuffers: finishedDataBuffers
    )
  }
}
