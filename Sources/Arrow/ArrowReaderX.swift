// Copyright 2025 The Apache Software Foundation
// Copyright 2025 The Columnar-Swift Contributors
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

import FlatBuffers
import Foundation

let fileMarker = Data("ARROW1".utf8)
let continuationMarker = UInt32(0xFFFF_FFFF)

public struct ArrowReaderX: Sendable {

  func makeBuffer(
    _ buffer: FBuffer,
    fileData: Data,
    length: UInt,
    messageOffset: Int64
  ) -> ArrowBuffer {
    let startOffset = messageOffset + buffer.offset
    let endOffset = startOffset + buffer.length

    //    let range = Int(startOffset)..<Int(endOffset)
    //    let offset = BorrowedOffsets(count: Int(length) / 4, data: fileData, range: range)
    // TODO: This should not copy.

    let bufferData = [UInt8](fileData[startOffset..<endOffset])
    return ArrowBuffer.createBuffer(bufferData, length: length)
  }

  private class RecordBatchData {
    let schema: FSchema
    let recordBatch: FRecordBatch
    private var fieldIndex: Int32 = 0
    private var nodeIndex: Int32 = 0
    private var bufferIndex: Int32 = 0
    init(
      _ recordBatch: FRecordBatch,
      schema: FSchema
    ) {
      self.recordBatch = recordBatch
      self.schema = schema
    }

    func nextNode() -> FFieldNode? {
      if nodeIndex >= self.recordBatch.nodesCount { return nil }
      defer { nodeIndex += 1 }
      return self.recordBatch.nodes(at: nodeIndex)
    }

    func nextBuffer() -> FBuffer? {
      if bufferIndex >= self.recordBatch.buffersCount { return nil }
      defer { bufferIndex += 1 }
      return self.recordBatch.buffers(at: bufferIndex)
    }

    func nextField() -> FField? {
      if fieldIndex >= self.schema.fieldsCount { return nil }
      defer { fieldIndex += 1 }
      return self.schema.fields(at: fieldIndex)
    }

    func isDone() -> Bool {
      nodeIndex >= self.recordBatch.nodesCount
    }
  }

  private struct DataLoadInfo {
    let fileData: Data
    let messageOffset: Int64
    var batchData: RecordBatchData
  }

  public class ArrowReaderResult {
    fileprivate var messageSchema: FSchema?
    public var schema: ArrowSchema?
    public var batches: [RecordBatchX] = []
  }

  public init() {}

  private func loadSchema(
    _ schema: FSchema
  ) -> Result<ArrowSchema, ArrowError> {
    let builder = ArrowSchema.Builder()
    for index in 0..<schema.fieldsCount {
      guard let field = schema.fields(at: index) else {
        return .failure(.init(.invalid("Field not found at index: \(index)")))
      }
      let fieldType: ArrowType
      do {
        fieldType = try findArrowType(field)
      } catch {
        return .failure(error)
      }
      guard let fieldName = field.name else {
        return .failure(.init(.invalid("Field name not found")))
      }
      let arrowField = ArrowField(
        name: fieldName,
        dataType: fieldType,
        isNullable: field.nullable
      )
      builder.addField(arrowField)
    }
    return .success(builder.finish())
  }

  private func loadStructData(
    _ loadInfo: DataLoadInfo,
    field: FField
  ) -> Result<AnyArrowArray, ArrowError> {
    guard let node = loadInfo.batchData.nextNode() else {
      return .failure(.init(.invalid("Node not found")))
    }

    guard let nullBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.init(.invalid("Null buffer not found")))
    }

    let nullLength = UInt(ceil(Double(node.length) / 8))
    let arrowNullBuffer = makeBuffer(
      nullBuffer, fileData: loadInfo.fileData,
      length: nullLength, messageOffset: loadInfo.messageOffset)
    var children: [ArrowData] = []
    for index in 0..<field.childrenCount {
      guard let childField = field.children(at: index) else {
        return .failure(
          .init(.invalid("Child field not found at index: \(index)")))
      }
      switch loadField(loadInfo, field: childField) {
      case .success(let holder):
        children.append(holder.arrowData)
      case .failure(let error):
        return .failure(error)
      }
    }
    return makeArrayHolder(
      field,
      buffers: [arrowNullBuffer],
      nullCount: UInt(node.nullCount),
      children: children,
      rbLength: UInt(loadInfo.batchData.recordBatch.length)
    )
  }

  private func loadListData(_ loadInfo: DataLoadInfo, field: FField)
    -> Result<AnyArrowArray, ArrowError>
  {
    guard let node = loadInfo.batchData.nextNode() else {
      return .failure(.init(.invalid("Node not found")))
    }

    guard let nullBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.init(.invalid("Null buffer not found")))
    }

    guard let offsetBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.init(.invalid("Offset buffer not found")))
    }

    let nullLength = UInt(ceil(Double(node.length) / 8))
    let arrowNullBuffer = makeBuffer(
      nullBuffer, fileData: loadInfo.fileData, length: nullLength,
      messageOffset: loadInfo.messageOffset)
    let arrowOffsetBuffer = makeBuffer(
      offsetBuffer, fileData: loadInfo.fileData, length: UInt(node.length + 1),
      messageOffset: loadInfo.messageOffset)

    guard field.childrenCount == 1, let childField = field.children(at: 0)
    else {
      return .failure(.init(.invalid("List must have exactly one child")))
    }

    switch loadField(loadInfo, field: childField) {
    case .success(let childHolder):
      return makeArrayHolder(
        field,
        buffers: [arrowNullBuffer, arrowOffsetBuffer],
        nullCount: UInt(node.nullCount),
        children: [childHolder.arrowData],
        rbLength: UInt(loadInfo.batchData.recordBatch.length))
    case .failure(let error):
      return .failure(error)
    }
  }

  private func loadPrimitiveData(
    _ loadInfo: DataLoadInfo,
    field: FField
  )
    -> Result<AnyArrowArray, ArrowError>
  {
    guard let node = loadInfo.batchData.nextNode() else {
      return .failure(.init(.invalid("Node not found")))
    }

    guard let nullBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.init(.invalid("Null buffer not found")))
    }

    guard let valueBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.init(.invalid("Value buffer not found")))
    }

    let nullLength = UInt(ceil(Double(node.length) / 8))
    let arrowNullBuffer = makeBuffer(
      nullBuffer,
      fileData: loadInfo.fileData,
      length: nullLength,
      messageOffset: loadInfo.messageOffset
    )
    let arrowValueBuffer = makeBuffer(
      valueBuffer,
      fileData: loadInfo.fileData,
      length: UInt(node.length),
      messageOffset: loadInfo.messageOffset
    )
    return makeArrayHolder(
      field,
      buffers: [arrowNullBuffer, arrowValueBuffer],
      nullCount: UInt(node.nullCount),
      children: nil,
      rbLength: UInt(loadInfo.batchData.recordBatch.length)
    )
  }

  // MARK: Variable data loading

  private func loadVariableData(
    _ loadInfo: DataLoadInfo,
    field: FField
  )
    -> Result<AnyArrowArray, ArrowError>
  {
    guard let node = loadInfo.batchData.nextNode() else {
      return .failure(.init(.invalid("Node not found")))
    }

    guard let nullBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.init(.invalid("Null buffer not found")))
    }

    guard let offsetBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.init(.invalid("Offset buffer not found")))
    }

    guard let valueBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.init(.invalid("Value buffer not found")))
    }

    let nullLength = UInt(ceil(Double(node.length) / 8))
    let arrowNullBuffer = makeBuffer(
      nullBuffer, fileData: loadInfo.fileData,
      length: nullLength, messageOffset: loadInfo.messageOffset)

    let arrowOffsetBuffer = makeBuffer(
      offsetBuffer, fileData: loadInfo.fileData,
      length: UInt(node.length), messageOffset: loadInfo.messageOffset)
    let arrowValueBuffer = makeBuffer(
      valueBuffer, fileData: loadInfo.fileData,
      length: UInt(node.length), messageOffset: loadInfo.messageOffset)
    return makeArrayHolder(
      field, buffers: [arrowNullBuffer, arrowOffsetBuffer, arrowValueBuffer],
      nullCount: UInt(node.nullCount), children: nil,
      rbLength: UInt(loadInfo.batchData.recordBatch.length))
  }

  private func loadField(
    _ loadInfo: DataLoadInfo,
    field: FField
  )
    -> Result<AnyArrowArray, ArrowError>
  {
    switch field.typeType {
    case .struct_:
      return loadStructData(loadInfo, field: field)
    case .list:
      return loadListData(loadInfo, field: field)
    default:
      if isFixedPrimitive(field.typeType) {
        return loadPrimitiveData(loadInfo, field: field)
      } else {
        return loadVariableData(loadInfo, field: field)
      }
    }
  }

  private func loadRecordBatch(
    _ recordBatch: FRecordBatch,
    schema: FSchema,
    arrowSchema: ArrowSchema,
    data: Data,
    messageEndOffset: Int64
  ) -> Result<RecordBatchX, ArrowError> {
    var columns: [AnyArrowArray] = []
    let batchData = RecordBatchData(recordBatch, schema: schema)
    let loadInfo = DataLoadInfo(
      fileData: data,
      messageOffset: messageEndOffset,
      batchData: batchData)
    while !batchData.isDone() {
      guard let field = batchData.nextField() else {
        return .failure(.init(.invalid("Field not found")))
      }

      let result = loadField(loadInfo, field: field)
      switch result {
      case .success(let holder):
        columns.append(holder)
      case .failure(let error):
        return .failure(error)
      }
    }
    return .success(RecordBatchX(arrowSchema, columns: columns))
  }

  /// This is for reading the Arrow streaming format.
  ///
  /// The Arrow streaming format is slightly different from the Arrow File format as it doesn't contain a
  /// header and footer.
  /// - Parameters:
  ///   - input: The buffer to read from
  ///   - useUnalignedBuffers: to be removed.
  /// - Returns: An `ArrowReaderResult` If successful, otherwise an `ArrowError`.
  public func readStreaming(
    _ input: Data,
    useUnalignedBuffers: Bool = false
  ) -> Result<ArrowReaderResult, ArrowError> {
    let result = ArrowReaderResult()
    var offset: Int = 0
    var length = getUInt32(input, offset: offset)
    var streamData = input
    // TODO: The following assumes message order will populate schemaMessage first
    var schemaMessage: FSchema?
    while length != 0 {
      if length == continuationMarker {
        offset += Int(MemoryLayout<UInt32>.size)
        length = getUInt32(input, offset: offset)
        if length == 0 {
          return .success(result)
        }
      }
      offset += Int(MemoryLayout<UInt32>.size)
      streamData = input[offset...]
      var dataBuffer = ByteBuffer(
        data: streamData
      )
      let message: FMessage = getRoot(byteBuffer: &dataBuffer)
      switch message.headerType {
      case .recordbatch:
        guard let rbMessage = message.header(type: FRecordBatch.self) else {
          return .failure(
            .init(.invalid("Failed to parse RecordBatch message")))
        }
        guard let schemaMessage else {
          return .failure(.init(.invalid("Schema message not found")))
        }
        guard let resultSchema = result.schema else {
          return .failure(.init(.invalid("Result schema not loaded")))
        }
        let recordBatchResult = loadRecordBatch(
          rbMessage,
          schema: schemaMessage,
          arrowSchema: resultSchema,
          data: input,
          messageEndOffset: Int64(offset) + Int64(length)
        )
        switch recordBatchResult {
        case .success(let recordBatch):
          result.batches.append(recordBatch)
        case .failure(let error):
          return .failure(error)
        }
        offset += Int(message.bodyLength + Int64(length))
        length = getUInt32(input, offset: offset)
      case .schema:
        schemaMessage = message.header(type: FSchema.self)
        guard let schemaMessage else {
          return .failure(.init(.invalid("Schema message not found")))
        }
        let schemaResult = loadSchema(schemaMessage)
        switch schemaResult {
        case .success(let schema):
          result.schema = schema
        case .failure(let error):
          return .failure(error)
        }
        offset += Int(message.bodyLength + Int64(length))
        length = getUInt32(input, offset: offset)
      default:
        return .failure(
          .init(.unknownError("Unhandled header type: \(message.headerType)")))
      }
    }
    return .success(result)
  }

  /// This is for reading the Arrow file format.
  ///
  /// The Arrow file format supports  random access. The Arrow file format contains a header and footer
  /// around the Arrow streaming format.
  /// - Parameters:
  ///   - fileData: the file content
  ///   - useUnalignedBuffers: to be removed.
  /// - Returns: An `ArrowReaderResult` on success, or an `ArrowError` on failure.
  public func readFile(
    _ fileData: Data,
    useUnalignedBuffers: Bool = false
  ) -> Result<ArrowReaderResult, ArrowError> {
    let footerLength = fileData.withUnsafeBytes { rawBuffer in
      rawBuffer.loadUnaligned(
        fromByteOffset: fileData.count - 4, as: Int32.self)
    }

    let result = ArrowReaderResult()
    let footerStartOffset = fileData.count - Int(footerLength + 4)
    let footerData = fileData[footerStartOffset...]
    var footerBuffer = ByteBuffer(
      data: footerData,
      allowReadingUnalignedBuffers: useUnalignedBuffers)
    let footer: FFooter = getRoot(byteBuffer: &footerBuffer)
    guard let footerSchema = footer.schema else {
      return .failure(.init(.invalid("Missing schema in footer")))
    }
    let schemaResult = loadSchema(footerSchema)
    switch schemaResult {
    case .success(let schema):
      result.schema = schema
    case .failure(let error):
      return .failure(error)
    }

    for index in 0..<footer.recordBatchesCount {
      guard let recordBatch: FBlock = footer.recordBatches(at: index) else {
        return .failure(
          .init(.invalid("Missing record batch at index \(index)")))
      }
      var messageLength = fileData.withUnsafeBytes { rawBuffer in
        rawBuffer.loadUnaligned(
          fromByteOffset: Int(recordBatch.offset), as: UInt32.self)
      }

      var messageOffset: Int64 = 1
      if messageLength == continuationMarker {
        messageOffset += 1
        messageLength = fileData.withUnsafeBytes { rawBuffer in
          rawBuffer.loadUnaligned(
            fromByteOffset: Int(
              recordBatch.offset + Int64(MemoryLayout<Int32>.size)),
            as: UInt32.self)
        }
      }

      let messageStartOffset =
        recordBatch.offset + (Int64(MemoryLayout<Int32>.size) * messageOffset)
      let messageEndOffset = messageStartOffset + Int64(messageLength)
      let recordBatchData = fileData[messageStartOffset..<messageEndOffset]
      var mbb = ByteBuffer(
        data: recordBatchData,
        allowReadingUnalignedBuffers: useUnalignedBuffers)
      let message: FMessage = getRoot(byteBuffer: &mbb)
      switch message.headerType {
      case .recordbatch:
        guard let rbMessage = message.header(type: FRecordBatch.self) else {
          return .failure(
            .init(.invalid("Expected RecordBatch as message header")))
        }
        guard let footerSchema = footer.schema else {
          return .failure(.init(.invalid("Expected schema in footer")))
        }
        // TODO: the result used here is also the return type. Ideally is would be constructed once as a struct.
        guard let resultSchema = result.schema else {
          return .failure(.init(.invalid("Expected schema in reader result")))
        }
        let recordBatchResult = loadRecordBatch(
          rbMessage,
          schema: footerSchema,
          arrowSchema: resultSchema,
          data: fileData,
          messageEndOffset: messageEndOffset
        )
        switch recordBatchResult {
        case .success(let recordBatch):
          result.batches.append(recordBatch)
        case .failure(let error):
          return .failure(error)
        }
      default:
        return .failure(
          .init(.unknownError("Unhandled header type: \(message.headerType)")))
      }
    }

    return .success(result)
  }

  public func fromFile(
    _ fileURL: URL
  ) -> Result<ArrowReaderResult, ArrowError> {
    do {
      // TODO: implement alignment checks.
      let fileData = try Data(contentsOf: fileURL, options: .mappedIfSafe)

      if !validateFileData(fileData) {
        return .failure(.init(.ioError("Not a valid arrow file.")))
      }
      let data = fileData[..<Int(fileData.count - 6)]
      return readFile(data)
    } catch {
      return .failure(.init(.unknownError("Error loading file: \(error)")))
    }
  }

  static public func makeArrowReaderResult() -> ArrowReaderResult {
    ArrowReaderResult()
  }

  public func fromMessage(
    _ dataHeader: Data,
    dataBody: Data,
    result: ArrowReaderResult,
    useUnalignedBuffers: Bool = false
  ) -> Result<Void, ArrowError> {
    var mbb = ByteBuffer(
      data: dataHeader,
      allowReadingUnalignedBuffers: useUnalignedBuffers)
    let message: FMessage = getRoot(byteBuffer: &mbb)
    switch message.headerType {
    case .schema:
      guard let sMessage = message.header(type: FSchema.self) else {
        return .failure(
          .init(.unknownError("Expected a schema but found none")))
      }
      switch loadSchema(sMessage) {
      case .success(let schema):
        result.schema = schema
        result.messageSchema = sMessage
        return .success(())
      case .failure(let error):
        return .failure(error)
      }
    case .recordbatch:
      guard let rbMessage = message.header(type: FRecordBatch.self) else {
        return .failure(
          .init(.invalid("Expected a RecordBatch but found none")))
      }
      // TODO: the result used here is also the return type. Ideally is would be constructed once as a struct (same issue as above)
      guard let messageSchema = result.messageSchema else {
        return .failure(
          .init(
            .invalid("Expected the result to have the messageSchema already")))
      }
      guard let resultSchema = result.schema else {
        return .failure(.init(.invalid("Expected result to have a schema")))
      }
      let recordBatchResult = loadRecordBatch(
        rbMessage,
        schema: messageSchema,
        arrowSchema: resultSchema,
        data: dataBody,
        messageEndOffset: 0
      )
      switch recordBatchResult {
      case .success(let recordBatch):
        result.batches.append(recordBatch)
        return .success(())
      case .failure(let error):
        return .failure(error)
      }
    default:
      return .failure(
        .init(.unknownError("Unhandled header type: \(message.headerType)")))
    }
  }

  func validateFileData(_ data: Data) -> Bool {
    let markerLength = fileMarker.count
    let startString = data[..<markerLength]
    let endString = data[(data.count - markerLength)...]
    return startString == fileMarker && endString == fileMarker
  }
}
