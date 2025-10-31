// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import FlatBuffers
import Foundation

public protocol DataWriter {
  var count: Int { get }
  func append(_ data: Data)
}

public class ArrowWriter {
  public class InMemDataWriter: DataWriter {
    public private(set) var data: Data
    public var count: Int { data.count }

    public init(_ data: Data) {
      self.data = data
    }

    convenience init() {
      self.init(Data())
    }

    public func append(_ data: Data) {
      self.data.append(data)
    }
  }

  public class FileDataWriter: DataWriter {
    private var handle: FileHandle
    private var currentSize: Int = 0
    public var count: Int { currentSize }
    public init(_ handle: FileHandle) {
      self.handle = handle
    }

    public func append(_ data: Data) {
      self.handle.write(data)
      self.currentSize += data.count
    }
  }

  public class Info {
    public let type: MessageHeader
    public let schema: ArrowSchema
    public let batches: [RecordBatch]

    public init(
      _ type: MessageHeader,
      schema: ArrowSchema,
      batches: [RecordBatch]
    ) {
      self.type = type
      self.schema = schema
      self.batches = batches
    }

    public convenience init(_ type: MessageHeader, schema: ArrowSchema) {
      self.init(type, schema: schema, batches: [RecordBatch]())
    }
  }

  public init() {}

  private func writeField(
    _ fbb: inout FlatBufferBuilder,
    field: ArrowField
  ) -> Result<Offset, ArrowError> {
    var fieldsOffset: Offset?
    if case .strct(let fields) = field.type {
      var offsets: [Offset] = []
      for field in fields {
        switch writeField(&fbb, field: field) {
        case .success(let offset):
          offsets.append(offset)
        case .failure(let error):
          return .failure(error)
        }
      }
      fieldsOffset = fbb.createVector(ofOffsets: offsets)
    }

    let nameOffset = fbb.create(string: field.name)
    let fieldTypeOffsetResult = toFBType(&fbb, arrowType: field.type)
    let startOffset = FlatField.startField(&fbb)
    FlatField.add(name: nameOffset, &fbb)
    FlatField.add(nullable: field.isNullable, &fbb)
    if let childrenOffset = fieldsOffset {
      FlatField.addVectorOf(children: childrenOffset, &fbb)
    }

    switch toFBTypeEnum(field.type) {
    case .success(let type):
      FlatField.add(typeType: type, &fbb)
    case .failure(let error):
      return .failure(error)
    }

    switch fieldTypeOffsetResult {
    case .success(let offset):
      FlatField.add(type: offset, &fbb)
      return .success(FlatField.endField(&fbb, start: startOffset))
    case .failure(let error):
      return .failure(error)
    }
  }

  private func writeSchema(
    _ fbb: inout FlatBufferBuilder,
    schema: ArrowSchema
  ) -> Result<Offset, ArrowError> {
    var fieldOffsets: [Offset] = []
    for field in schema.fields {
      switch writeField(&fbb, field: field) {
      case .success(let offset):
        fieldOffsets.append(offset)
      case .failure(let error):
        return .failure(error)
      }
    }
    let fieldsOffset: Offset = fbb.createVector(ofOffsets: fieldOffsets)
    let schemaOffset =
      Schema.createSchema(
        &fbb,
        endianness: .little,
        fieldsVectorOffset: fieldsOffset
      )
    return .success(schemaOffset)
  }

  private func writeRecordBatches(
    _ writer: inout DataWriter,
    batches: [RecordBatch]
  ) -> Result<[Block], ArrowError> {
    var rbBlocks: [Block] = .init()
    for batch in batches {
      let startIndex = writer.count
      switch writeRecordBatch(batch: batch) {
      case .success(let rbResult):
        withUnsafeBytes(of: continuationMarker.littleEndian) {
          writer.append(Data($0))
        }
        withUnsafeBytes(of: rbResult.1.o.littleEndian) {
          writer.append(Data($0))
        }
        writer.append(rbResult.0)
        addPadForAlignment(&writer)
        let metadataLength = writer.count - startIndex
        let bodyStart = writer.count
        switch writeRecordBatchData(
          &writer,
          fields: batch.schema.fields,
          columns: batch.columns
        ) {
        case .success:
          let bodyLength = writer.count - bodyStart
          let expectedSize = startIndex + metadataLength + bodyLength
          guard expectedSize == writer.count else {
            return .failure(
              .invalid(
                "Invalid Block. Expected \(expectedSize), got \(writer.count)"
              ))
          }
          rbBlocks.append(
            Block(
              offset: Int64(startIndex),
              metaDataLength: Int32(metadataLength),
              bodyLength: Int64(bodyLength)
            )
          )
        case .failure(let error):
          return .failure(error)
        }
      case .failure(let error):
        return .failure(error)
      }
    }

    return .success(rbBlocks)
  }

  private func writeFieldNodes(
    _ fields: [ArrowField],
    columns: [AnyArrowArray],
    offsets: inout [Offset],
    fbb: inout FlatBufferBuilder
  ) {
    for index in (0..<fields.count).reversed() {
      let column = columns[index]
      let fieldNode = FieldNode(
        length: Int64(column.length),
        nullCount: Int64(column.nullCount)
      )
      offsets.append(fbb.create(struct: fieldNode))
      if case .strct(let fields) = column.type {

        let nestedArray = column as? NestedArray
        if let nestedFields = nestedArray?.fields {
          writeFieldNodes(
            fields,
            columns: nestedFields,
            offsets: &offsets,
            fbb: &fbb
          )
        }
      }
    }
  }

  private func writeBufferInfo(
    _ fields: [ArrowField],
    columns: [AnyArrowArray],
    bufferOffset: inout Int,
    buffers: inout [Buffer],
    fbb: inout FlatBufferBuilder
  ) {
    for index in 0..<fields.count {
      let column = columns[index]
      for var bufferDataSize in column.bufferDataSizes {
        bufferDataSize = getPadForAlignment(bufferDataSize)
        let buffer = Buffer(
          offset: Int64(bufferOffset), length: Int64(bufferDataSize))
        buffers.append(buffer)
        bufferOffset += bufferDataSize

        if case .strct(let fields) = column.type {
          let nestedArray = column as? NestedArray
          if let nestedFields = nestedArray?.fields {
            writeBufferInfo(
              fields, columns: nestedFields,
              bufferOffset: &bufferOffset, buffers: &buffers, fbb: &fbb)
          }
        }
      }
    }
  }

  private func writeRecordBatch(
    batch: RecordBatch
  ) -> Result<(Data, Offset), ArrowError> {
    let schema = batch.schema
    var fbb = FlatBufferBuilder()

    // write out field nodes
    var fieldNodeOffsets: [Offset] = []
    fbb.startVector(
      schema.fields.count,
      elementSize: MemoryLayout<FieldNode>.size
    )
    writeFieldNodes(
      schema.fields,
      columns: batch.columns,
      offsets: &fieldNodeOffsets,
      fbb: &fbb
    )
    let nodeOffset = fbb.endVector(len: fieldNodeOffsets.count)
    // write out buffers
    var buffers: [Buffer] = .init()
    var bufferOffset: Int = 0
    writeBufferInfo(
      schema.fields, columns: batch.columns,
      bufferOffset: &bufferOffset, buffers: &buffers,
      fbb: &fbb
    )
    FlatRecordBatch.startVectorOfBuffers(batch.schema.fields.count, in: &fbb)
    for buffer in buffers.reversed() {
      fbb.create(struct: buffer)
    }
    let batchBuffersOffset = fbb.endVector(len: buffers.count)
    let startRb = FlatRecordBatch.startRecordBatch(&fbb)
    FlatRecordBatch.addVectorOf(nodes: nodeOffset, &fbb)
    FlatRecordBatch.addVectorOf(buffers: batchBuffersOffset, &fbb)
    FlatRecordBatch.add(length: Int64(batch.length), &fbb)
    let recordBatchOffset = FlatRecordBatch.endRecordBatch(
      &fbb,
      start: startRb
    )
    let bodySize = Int64(bufferOffset)
    let startMessage = Message.startMessage(&fbb)
    Message.add(version: .max, &fbb)
    Message.add(bodyLength: Int64(bodySize), &fbb)
    Message.add(headerType: .recordbatch, &fbb)
    Message.add(header: recordBatchOffset, &fbb)
    let messageOffset = Message.endMessage(&fbb, start: startMessage)
    fbb.finish(offset: messageOffset)
    return .success((fbb.data, Offset(offset: UInt32(fbb.data.count))))
  }

  private func writeRecordBatchData(
    _ writer: inout DataWriter, fields: [ArrowField],
    columns: [AnyArrowArray]
  ) -> Result<Bool, ArrowError> {
    for index in 0..<fields.count {
      let column = columns[index]
      let colBufferData = column.bufferData
      for var bufferData in colBufferData {
        addPadForAlignment(&bufferData)
        writer.append(bufferData)
        if case .strct(let fields) = column.type {
          guard let nestedArray = column as? NestedArray,
            let nestedFields = nestedArray.fields
          else {
            return .failure(
              .invalid("Struct type array expected for nested type")
            )
          }
          switch writeRecordBatchData(
            &writer,
            fields: fields,
            columns: nestedFields
          ) {
          case .success:
            continue
          case .failure(let error):
            return .failure(error)
          }
        }
      }
    }
    return .success(true)
  }

  private func writeFooter(
    schema: ArrowSchema,
    rbBlocks: [Block]
  ) -> Result<Data, ArrowError> {
    var fbb: FlatBufferBuilder = FlatBufferBuilder()
    switch writeSchema(&fbb, schema: schema) {
    case .success(let schemaOffset):
      fbb.startVector(
        rbBlocks.count, elementSize: MemoryLayout<Block>.size)
      for blkInfo in rbBlocks.reversed() {
        fbb.create(struct: blkInfo)
      }
      let rbBlkEnd = fbb.endVector(len: rbBlocks.count)
      let footerStartOffset = Footer.startFooter(&fbb)
      Footer.add(schema: schemaOffset, &fbb)
      Footer.addVectorOf(recordBatches: rbBlkEnd, &fbb)
      let footerOffset = Footer.endFooter(&fbb, start: footerStartOffset)
      fbb.finish(offset: footerOffset)
      return .success(fbb.data)
    case .failure(let error):
      return .failure(error)
    }
  }

  private func writeFile(
    _ writer: inout DataWriter,
    info: ArrowWriter.Info
  ) -> Result<Bool, ArrowError> {
    var fbb: FlatBufferBuilder = FlatBufferBuilder()
    switch writeSchema(&fbb, schema: info.schema) {
    case .success(let schemaOffset):
      fbb.finish(offset: schemaOffset)
      writer.append(fbb.data)
      addPadForAlignment(&writer)
    case .failure(let error):
      return .failure(error)
    }
    switch writeRecordBatches(&writer, batches: info.batches) {
    case .success(let rbBlocks):
      switch writeFooter(schema: info.schema, rbBlocks: rbBlocks) {
      case .success(let footerData):
        fbb.finish(offset: Offset(offset: fbb.buffer.size))
        let footerOffset = writer.count
        writer.append(footerData)
        addPadForAlignment(&writer)
        withUnsafeBytes(of: Int32(0).littleEndian) {
          writer.append(Data($0))
        }
        let footerDiff = (UInt32(writer.count) - UInt32(footerOffset))
        withUnsafeBytes(of: footerDiff.littleEndian) {
          writer.append(Data($0))
        }
      case .failure(let error):
        return .failure(error)
      }
    case .failure(let error):
      return .failure(error)
    }

    return .success(true)
  }

  public func writeStreaming(
    _ info: ArrowWriter.Info
  ) -> Result<Data, ArrowError> {
    let writer: any DataWriter = InMemDataWriter()
    switch toMessage(info.schema) {
    case .success(let schemaData):
      withUnsafeBytes(of: continuationMarker.littleEndian) {
        writer.append(Data($0))
      }
      withUnsafeBytes(of: UInt32(schemaData.count).littleEndian) {
        writer.append(Data($0))
      }
      writer.append(schemaData)
    case .failure(let error):
      return .failure(error)
    }

    for batch in info.batches {
      switch toMessage(batch) {
      case .success(let batchData):
        withUnsafeBytes(of: continuationMarker.littleEndian) {
          writer.append(Data($0))
        }
        withUnsafeBytes(of: UInt32(batchData[0].count).littleEndian) {
          writer.append(Data($0))
        }
        writer.append(batchData[0])
        writer.append(batchData[1])
      case .failure(let error):
        return .failure(error)
      }
    }
    withUnsafeBytes(of: continuationMarker.littleEndian) {
      writer.append(Data($0))
    }
    withUnsafeBytes(of: UInt32(0).littleEndian) {
      writer.append(Data($0))
    }
    if let memWriter = writer as? InMemDataWriter {
      return .success(memWriter.data)
    } else {
      return .failure(.invalid("Unable to cast writer"))
    }
  }

  public func writeFile(_ info: ArrowWriter.Info) -> Result<Data, ArrowError> {
    var writer: any DataWriter = InMemDataWriter()
    switch writeFile(&writer, info: info) {
    case .success:
      if let memWriter = writer as? InMemDataWriter {
        return .success(memWriter.data)
      } else {
        return .failure(.invalid("Unable to cast writer"))
      }
    case .failure(let error):
      return .failure(error)
    }
  }

  public func toFile(
    _ fileName: URL,
    info: ArrowWriter.Info
  ) -> Result<Bool, ArrowError> {
    do {
      try Data().write(to: fileName)
    } catch {
      return .failure(.ioError("\(error)"))
    }
    guard let fileHandle = FileHandle(forUpdatingAtPath: fileName.path) else {
      return .failure(.ioError("Unable to open \(fileName.path) for writing"))
    }
    defer { fileHandle.closeFile() }

    var markerData = fileMarker
    addPadForAlignment(&markerData)

    var writer: any DataWriter = FileDataWriter(fileHandle)
    writer.append(markerData)
    switch writeFile(&writer, info: info) {
    case .success:
      writer.append(fileMarker)
    case .failure(let error):
      return .failure(error)
    }
    return .success(true)
  }

  public func toMessage(_ batch: RecordBatch) -> Result<[Data], ArrowError> {
    var writer: any DataWriter = InMemDataWriter()
    switch writeRecordBatch(batch: batch) {
    case .success(let message):
      writer.append(message.0)
      addPadForAlignment(&writer)
      var dataWriter: any DataWriter = InMemDataWriter()
      switch writeRecordBatchData(
        &dataWriter, fields: batch.schema.fields, columns: batch.columns)
      {
      case .success:
        guard let inMemWriter = writer as? InMemDataWriter,
          let inMemDataWriter = dataWriter as? InMemDataWriter
        else {
          return .failure(.invalid("Unable to cast writer"))
        }
        return .success([
          inMemWriter.data,
          inMemDataWriter.data,
        ])
      case .failure(let error):
        return .failure(error)
      }
    case .failure(let error):
      return .failure(error)
    }
  }

  public func toMessage(_ schema: ArrowSchema) -> Result<Data, ArrowError> {
    var schemaSize: Int32 = 0
    var fbb = FlatBufferBuilder()
    switch writeSchema(&fbb, schema: schema) {
    case .success(let schemaOffset):
      schemaSize = Int32(schemaOffset.o)
    case .failure(let error):
      return .failure(error)
    }

    let startMessage = Message.startMessage(&fbb)
    Message.add(bodyLength: Int64(0), &fbb)
    Message.add(headerType: .schema, &fbb)
    Message.add(header: Offset(offset: UOffset(schemaSize)), &fbb)
    Message.add(version: .max, &fbb)
    let messageOffset = Message.endMessage(&fbb, start: startMessage)
    fbb.finish(offset: messageOffset)
    return .success(fbb.data)
  }
}
