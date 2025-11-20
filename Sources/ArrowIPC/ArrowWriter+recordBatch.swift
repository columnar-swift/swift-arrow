// Copyright 2025 The Apache Software Foundation
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

extension ArrowWriter {

  private mutating func writeRecordBatches(
    batches: [RecordBatch]
  ) throws -> [FBlock] {
    var rbBlocks: [FBlock] = .init()
    for batch in batches {

      let startIndex = data.count

      let message = try write(batch: batch)
      var buffer = Data()
      withUnsafeBytes(of: continuationMarker.littleEndian) { val in
        buffer.append(contentsOf: val)
      }
      withUnsafeBytes(of: UInt32(message.count).littleEndian) { val in
        buffer.append(contentsOf: val)
      }
      // padded
      write(data: buffer)
      let metadataLength = data.count - startIndex
      let bodyStart = data.count

    }
    ////          {
    //        case .success(let rbResult):
    //          withUnsafeBytes(of: continuationMarker.littleEndian) {
    //            writer.append(Data($0))
    //          }
    //          withUnsafeBytes(of: UInt32(rbResult.count).littleEndian) {
    //            writer.append(Data($0))
    //          }
    //          writer.append(rbResult)
    //          addPadForAlignment(&writer)
    //          let metadataLength = writer.count - startIndex
    //          let bodyStart = writer.count
    //          switch writeRecordBatchData(
    //            &writer,
    //            fields: batch.schema.fields,
    //            columns: batch.columns
    //          ) {
    //          case .success:
    //            let bodyLength = writer.count - bodyStart
    //            let expectedSize = startIndex + metadataLength + bodyLength
    //            guard expectedSize == writer.count else {
    //              return .failure(
    //                .invalid(
    //                  "Invalid Block. Expected \(expectedSize), got \(writer.count)"
    //                ))
    //            }
    //            rbBlocks.append(
    //              FBlock(
    //                offset: Int64(startIndex),
    //                metaDataLength: Int32(metadataLength),
    //                bodyLength: Int64(bodyLength)
    //              )
    //            )
    //          case .failure(let error):
    //            return .failure(error)
    //          }
    //        case .failure(let error):
    //          return .failure(error)
    //        }
    //      }
    //
    //      return .success(rbBlocks)
    fatalError()
  }

  private mutating func writeRecordBatchData(
    fields: [ArrowField],
    columns: [any ArrowArrayProtocol]
  ) throws {
    for index in 0..<fields.count {
      let column = columns[index]
      let field = fields[index]
      //      let colBufferData = column.bufferData
      // FIXME: Maybe separate data and array protocols
      let colBufferData: [Data] = [Data()]
      for bufferData in colBufferData {
        write(data: bufferData)
        if case .strct(let fields) = field.type {
          guard let nestedArray = column as? ArrowStructArray
          else {
            throw ArrowError.invalid(
              "Struct type array expected for nested type")
          }
          try writeRecordBatchData(
            fields: fields,
            columns: nestedArray.fields.map(\.array)
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
    columns: [any ArrowArrayProtocol],
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
    columns: [any ArrowArrayProtocol],
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
