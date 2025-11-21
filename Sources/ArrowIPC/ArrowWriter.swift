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
import FlatBuffers
import Foundation

public struct ArrowWriter {

  let url: URL
  var data: Data = .init()

  public init(url: URL) {
    self.url = url
    write(bytes: fileMarker)
  }

  mutating func finish() throws {
    data.append(contentsOf: fileMarker)
    try data.write(to: url)
  }

  func padded(byteCount: Int, alignment: Int = 8) -> Int {
    let padding = byteCount % alignment
    if padding > 0 {
      return byteCount + alignment - padding
    }
    return byteCount
  }

  mutating func pad(alignment: Int = 8) {
    let remainder = data.count % alignment
    if remainder > 0 {
      let padding = alignment - remainder
      data.append(contentsOf: [UInt8](repeating: 0, count: padding))
    }
  }

  mutating func write(bytes: [UInt8], alignment: Int = 8) {
    data.append(contentsOf: fileMarker)
    let remainder = bytes.count % alignment
    if remainder > 0 {
      let padding = alignment - remainder
      data.append(contentsOf: [UInt8](repeating: 0, count: padding))
    }
    precondition(data.count % 8 == 0, "File must be aligned to 8 bytes.")
  }

  mutating func write(data other: Data, alignment: Int = 8) {
    self.data.append(other)
    let remainder = data.count % alignment
    if remainder > 0 {
      let padding = alignment - remainder
      data.append(contentsOf: [UInt8](repeating: 0, count: padding))
    }
    precondition(data.count % 8 == 0, "File must be aligned to 8 bytes.")
  }

  mutating func write(
    schema: ArrowSchema,
    recordBatches: [RecordBatch]
  ) throws {

    try write(schema: schema)
    precondition(data.count % 8 == 0)
    let blocks = try write(recordBatches: recordBatches)

    precondition(data.count % 8 == 0)
    let footerOffset = data.count
    let footerData = try writeFooter(schema: schema, blocks: blocks)
    write(data: footerData)
    precondition(data.count % 8 == 0)
    withUnsafeBytes(of: Int32.zero.littleEndian) { val in
      data.append(contentsOf: val)
    }
    let footerLength = data.count - footerOffset
    withUnsafeBytes(of: Int32(footerLength).littleEndian) { val in
      data.append(contentsOf: val)
    }
  }

  mutating func write(schema: ArrowSchema) throws(ArrowError) {
    var fbb: FlatBufferBuilder = .init()
    let schemaOffset = try write(schema: schema, to: &fbb)
    fbb.finish(offset: schemaOffset)
    self.write(data: fbb.data)
  }

  /// Write the schema to file.
  /// - Parameters:
  ///   - schema:The Arrow schema
  ///   - fbb: the FlatBuffers builder to append the schema this to.
  /// - Returns: The FlatBuffers offset.
  /// - Throws: An `ArrowError` if type conversion is unable to continue.
  private func write(
    schema: ArrowSchema,
    to fbb: inout FlatBufferBuilder
  ) throws(ArrowError) -> Offset {
    var fieldOffsets: [Offset] = []
    for field in schema.fields {
      let offset = try write(field: field, to: &fbb)
      fieldOffsets.append(offset)
    }
    let fieldsOffset: Offset = fbb.createVector(ofOffsets: fieldOffsets)
    let schemaOffset = FSchema.createSchema(
      &fbb,
      endianness: .little,
      fieldsVectorOffset: fieldsOffset
    )
    return schemaOffset
  }

  private func writeFooter(
    schema: ArrowSchema,
    blocks: [FBlock]
  ) throws(ArrowError) -> Data {
    var fbb: FlatBufferBuilder = .init()
    let schemaOffset = try write(schema: schema, to: &fbb)
    fbb.startVector(
      blocks.count,
      elementSize: MemoryLayout<FBlock>.size
    )
    for block in blocks.reversed() {
      fbb.create(struct: block)
    }
    let blocksOffset = fbb.endVector(len: blocks.count)
    let footerStartOffset = FFooter.startFooter(&fbb)
    FFooter.add(schema: schemaOffset, &fbb)
    FFooter.addVectorOf(recordBatches: blocksOffset, &fbb)
    let footerOffset = FFooter.endFooter(&fbb, start: footerStartOffset)
    fbb.finish(offset: footerOffset)
    return fbb.data
  }

  // MARK: Record batch methods.

  private mutating func write(
    recordBatches: [RecordBatch]
  ) throws -> [FBlock] {
    var blocks: [FBlock] = .init()
    for recordBatch in recordBatches {

      let startIndex = data.count

      let message = try write(batch: recordBatch)
      // TODO: Better API for marker/count
      var buffer = Data()
      withUnsafeBytes(of: continuationMarker.littleEndian) { val in
        buffer.append(contentsOf: val)
      }
      withUnsafeBytes(of: UInt32(message.count).littleEndian) { val in
        buffer.append(contentsOf: val)
      }
      write(data: buffer)
      write(data: message)
      precondition(data.count % 8 == 0)
      let metadataLength = data.count - startIndex
      let bodyStart = data.count

      try writeRecordBatchData(
        fields: recordBatch.schema.fields,
        arrays: recordBatch.columns
      )
      precondition(data.count % 8 == 0)

      let bodyLength = data.count - bodyStart
      let expectedSize = startIndex + metadataLength + bodyLength
      guard expectedSize == data.count else {
        throw ArrowError.invalid(
          "Invalid Block. Expected \(expectedSize), got \(data.count)"
        )
      }
      blocks.append(
        FBlock(
          offset: Int64(startIndex),
          metaDataLength: Int32(metadataLength),
          bodyLength: Int64(bodyLength)
        )
      )
    }
    return blocks
  }

  private mutating func writeRecordBatchData(
    fields: [ArrowField],
    arrays: [AnyArrowArrayProtocol]
  ) throws {
    for index in 0..<fields.count {
      let array = arrays[index]
      let field = fields[index]
      let buffers = array.buffers

      for buffer in buffers {
        buffer.withUnsafeBytes { ptr in
          data.append(contentsOf: ptr)
        }
        pad()
        precondition(data.count % 8 == 0, "Data size must be multiple of 8")
        if case .strct(let fields) = field.type {
          guard let structArray = array as? ArrowStructArray
          else {
            throw ArrowError.invalid(
              "Struct type array expected for nested type")
          }
          try writeRecordBatchData(
            fields: fields,
            arrays: structArray.fields.map(\.array)
          )
        }
      }
    }
  }

  private func write(
    batch: RecordBatch
  ) throws -> Data {
    let schema = batch.schema
    var fbb = FlatBufferBuilder()

    // MARK: Field nodes.
    var fieldNodeOffsets: [Offset] = []
    fbb.startVector(
      schema.fields.count,
      elementSize: MemoryLayout<FFieldNode>.size
    )
    writeFieldNodes(
      fields: schema.fields,
      columns: batch.columns,
      offsets: &fieldNodeOffsets,
      fbb: &fbb
    )
    let nodeOffset = fbb.endVector(len: fieldNodeOffsets.count)

    // MARK: Buffers.
    var buffers: [FBuffer] = .init()
    var bufferOffset: Int = 0
    writeBufferInfo(
      schema.fields, columns: batch.columns,
      bufferOffset: &bufferOffset, buffers: &buffers,
      fbb: &fbb
    )
    FRecordBatch.startVectorOfBuffers(batch.schema.fields.count, in: &fbb)
    for buffer in buffers.reversed() {
      fbb.create(struct: buffer)
    }
    let batchBuffersOffset = fbb.endVector(len: buffers.count)
    let startRb = FRecordBatch.startRecordBatch(&fbb)
    FRecordBatch.addVectorOf(nodes: nodeOffset, &fbb)
    FRecordBatch.addVectorOf(buffers: batchBuffersOffset, &fbb)
    FRecordBatch.add(length: Int64(batch.length), &fbb)
    let recordBatchOffset = FRecordBatch.endRecordBatch(
      &fbb,
      start: startRb
    )
    let bodySize = Int64(bufferOffset)
    let startMessage = FMessage.startMessage(&fbb)
    FMessage.add(version: .max, &fbb)
    FMessage.add(bodyLength: Int64(bodySize), &fbb)
    FMessage.add(headerType: .recordbatch, &fbb)
    FMessage.add(header: recordBatchOffset, &fbb)
    let messageOffset = FMessage.endMessage(&fbb, start: startMessage)
    fbb.finish(offset: messageOffset)
    return fbb.data
  }

  private func writeFieldNodes(
    fields: [ArrowField],
    columns: [AnyArrowArrayProtocol],
    offsets: inout [Offset],
    fbb: inout FlatBufferBuilder
  ) {
    for index in (0..<fields.count).reversed() {
      let column = columns[index]
      let field = fields[index]
      let fieldNode = FFieldNode(
        length: Int64(column.length),
        nullCount: Int64(column.nullCount)
      )
      offsets.append(fbb.create(struct: fieldNode))
      if case .strct(let fields) = field.type {
        if let column = column as? ArrowStructArray {
          writeFieldNodes(
            fields: fields,
            columns: column.fields.map(\.array),
            offsets: &offsets,
            fbb: &fbb
          )
        }
      }
    }
  }

  private func writeBufferInfo(
    _ fields: [ArrowField],
    columns: [AnyArrowArrayProtocol],
    bufferOffset: inout Int,
    buffers: inout [FBuffer],
    fbb: inout FlatBufferBuilder
  ) {
    for index in 0..<fields.count {
      let column = columns[index]
      let field = fields[index]
      for var bufferDataSize in column.bufferSizes {
        bufferDataSize = padded(byteCount: bufferDataSize)
        let buffer = FBuffer(
          offset: Int64(bufferOffset), length: Int64(bufferDataSize))
        buffers.append(buffer)
        bufferOffset += bufferDataSize

        if case .strct(let fields) = field.type {

          if let column = column as? ArrowStructArray {

            writeBufferInfo(
              fields,
              columns: column.fields.map(\.array),
              bufferOffset: &bufferOffset,
              buffers: &buffers, fbb: &fbb
            )
          }
        }
      }
    }
  }
}
