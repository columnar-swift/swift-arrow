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

public struct ArrowArrayBinaryView<ItemType: VariableLength>: ArrowArrayProtocol
{
  public let offset: Int
  public let length: Int
  private let nullBuffer: NullBuffer
  private let viewsBuffer: any FixedWidthBufferProtocol<BinaryView>
  let dataBuffers: [any VariableLengthBufferProtocol<ItemType>]

  public var bufferSizes: [Int] {
    [nullBuffer.length, viewsBuffer.length] + dataBuffers.map { $0.length }
  }

  public var buffers: [ArrowBufferProtocol] {
    [nullBuffer, viewsBuffer] + dataBuffers.map { $0 as ArrowBufferProtocol }
  }

  public var nullCount: Int { nullBuffer.nullCount }

  public init<Views: FixedWidthBufferProtocol<BinaryView>>(
    offset: Int = 0,
    length: Int,
    nullBuffer: NullBuffer,
    viewsBuffer: Views,
    dataBuffers: [any VariableLengthBufferProtocol<ItemType>]
  ) {
    self.offset = offset
    self.length = length
    self.nullBuffer = nullBuffer
    self.viewsBuffer = viewsBuffer
    self.dataBuffers = dataBuffers
  }

  public subscript(index: Int) -> ItemType? {
    let offsetIndex = self.offset + index
    guard self.nullBuffer.isSet(offsetIndex) else {
      return nil
    }

    let view = viewsBuffer[offsetIndex]

    if view.isInline {
      // Fast path: data is inline
      return view.withInlineData { dataSpan in
        dataSpan.withUnsafeBufferPointer { buffer in
          ItemType(buffer)
        }
      }
    } else {
      // Referenced data
      let bufferIndex = Int(view.bufferIndex)
      let offset = Int(view.offset)

      precondition(
        bufferIndex >= 0 && bufferIndex < dataBuffers.count,
        "Invalid buffer index")

      let dataBuffer = dataBuffers[bufferIndex]
      return dataBuffer.loadVariable(
        at: offset,
        arrayLength: Int(view.length)
      )
    }
  }

  public func slice(offset: Int, length: Int) -> Self {
    // True zero-copy: just adjust offset/length, share all buffers
    .init(
      offset: offset,
      length: length,
      nullBuffer: nullBuffer,
      viewsBuffer: viewsBuffer,
      dataBuffers: dataBuffers
    )
  }

  /// Compact the array by copying referenced data into fewer buffers.
  public func compact() -> Self {
    // TODO: Implement compaction strategy
    // For now, just return self
    self
  }

  /// Get buffer utilization statistics.
  public func bufferStats() -> [(bufferIndex: Int, utilization: Double)] {
    // TODO: Track which views reference which buffers
    []
  }
}
