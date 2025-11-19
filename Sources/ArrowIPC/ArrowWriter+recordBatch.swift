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

  private func write(
    batch: RecordBatch
  ) throws -> Offset {
    let schema = batch.schema
    var fbb = FlatBufferBuilder()

    // write out field nodes
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
    // write out buffers
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
    //    return .success((fbb.data, Offset(offset: UInt32(fbb.data.count))))
    fatalError()
  }

  private func writeFieldNodes(
    fields: [ArrowField],
    columns: [any ArrowArrayProtocol],
    offsets: inout [Offset],
    fbb: inout FlatBufferBuilder
  ) {
    fatalError()
    //    for index in (0..<fields.count).reversed() {
    //      let column = columns[index]
    //      let field = fields[index]
    //      let fieldNode = FFieldNode(
    //        length: Int64(column.length),
    //        nullCount: Int64(column.nullCount)
    //      )
    //      offsets.append(fbb.create(struct: fieldNode))
    //      if case .strct(let fields) = field.type {
    //
    //        if let column = column as? ArrowStructArray {
    //          writeFieldNodes(
    //            fields: fields,
    //            columns: column.fields.map(\.array),
    //            offsets: &offsets,
    //            fbb: &fbb
    //          )
    //        }
    //      }
    //    }
  }

  private func writeBufferInfo(
    _ fields: [ArrowField],
    columns: [any ArrowArrayProtocol],
    bufferOffset: inout Int,
    buffers: inout [FBuffer],
    fbb: inout FlatBufferBuilder
  ) {
    fatalError()
    //    for index in 0..<fields.count {
    //      let column = columns[index]
    //      let field = fields[index]
    //      for var bufferDataSize in column.bufferDataSizes {
    //
    //        bufferDataSize = getPadForAlignment(bufferDataSize)
    //        let buffer = FBuffer(
    //          offset: Int64(bufferOffset), length: Int64(bufferDataSize))
    //        buffers.append(buffer)
    //        bufferOffset += bufferDataSize
    //
    //        if case .strct(let fields) = column.type {
    //          let nestedArray = column as? NestedArray
    //          if let nestedFields = nestedArray?.fields {
    //            writeBufferInfo(
    //              fields, columns: nestedFields,
    //              bufferOffset: &bufferOffset, buffers: &buffers, fbb: &fbb)
    //          }
    //        }
    //      }
    //    }
  }
}
