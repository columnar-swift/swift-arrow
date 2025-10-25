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

let fileMarker = "ARROW1"
let continuationMarker = UInt32(0xFFFF_FFFF)

public class ArrowReader {
  private class RecordBatchData {
    let schema: Schema
    let recordBatch: FlatRecordBatch
    private var fieldIndex: Int32 = 0
    private var nodeIndex: Int32 = 0
    private var bufferIndex: Int32 = 0
    init(
      _ recordBatch: FlatRecordBatch,
      schema: Schema
    ) {
      self.recordBatch = recordBatch
      self.schema = schema
    }

    func nextNode() -> FieldNode? {
      if nodeIndex >= self.recordBatch.nodesCount { return nil }
      defer { nodeIndex += 1 }
      return self.recordBatch.nodes(at: nodeIndex)
    }

    func nextBuffer() -> Buffer? {
      if bufferIndex >= self.recordBatch.buffersCount { return nil }
      defer { bufferIndex += 1 }
      return self.recordBatch.buffers(at: bufferIndex)
    }

    func nextField() -> FlatField? {
      if fieldIndex >= self.schema.fieldsCount { return nil }
      defer { fieldIndex += 1 }
      return self.schema.fields(at: fieldIndex)
    }

    func isDone() -> Bool {
      return nodeIndex >= self.recordBatch.nodesCount
    }
  }

  private struct DataLoadInfo {
    let fileData: Data
    let messageOffset: Int64
    var batchData: RecordBatchData
  }

  public class ArrowReaderResult {
    fileprivate var messageSchema: Schema?
    public var schema: ArrowSchema?
    public var batches = [RecordBatch]()
  }

  public init() {}

  private func loadSchema(
    _ schema: Schema
  ) -> Result<ArrowSchema, ArrowError> {
    let builder = ArrowSchema.Builder()
    for index in 0..<schema.fieldsCount {
      let field = schema.fields(at: index)!
      let fieldType = findArrowType(field)
      if fieldType.info == ArrowType.arrowUnknown {
        return .failure(.unknownType("Unsupported field type found: \(field.typeType)"))
      }
      let arrowField = ArrowField(field.name!, type: fieldType, isNullable: field.nullable)
      builder.addField(arrowField)
    }

    return .success(builder.finish())
  }

  private func loadStructData(
    _ loadInfo: DataLoadInfo,
    field: FlatField
  ) -> Result<ArrowArrayHolder, ArrowError> {
    guard let node = loadInfo.batchData.nextNode() else {
      return .failure(.invalid("Node not found"))
    }

    guard let nullBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.invalid("Null buffer not found"))
    }

    let nullLength = UInt(ceil(Double(node.length) / 8))
    let arrowNullBuffer = makeBuffer(
      nullBuffer, fileData: loadInfo.fileData,
      length: nullLength, messageOffset: loadInfo.messageOffset)
    var children = [ArrowData]()
    for index in 0..<field.childrenCount {
      let childField = field.children(at: index)!
      switch loadField(loadInfo, field: childField) {
      case .success(let holder):
        children.append(holder.array.arrowData)
      case .failure(let error):
        return .failure(error)
      }
    }

    return makeArrayHolder(
      field, buffers: [arrowNullBuffer],
      nullCount: UInt(node.nullCount), children: children,
      rbLength: UInt(loadInfo.batchData.recordBatch.length))
  }

  private func loadListData(_ loadInfo: DataLoadInfo, field: FlatField)
    -> Result<ArrowArrayHolder, ArrowError>
  {
    guard let node = loadInfo.batchData.nextNode() else {
      return .failure(.invalid("Node not found"))
    }

    guard let nullBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.invalid("Null buffer not found"))
    }

    guard let offsetBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.invalid("Offset buffer not found"))
    }

    let nullLength = UInt(ceil(Double(node.length) / 8))
    let arrowNullBuffer = makeBuffer(
      nullBuffer, fileData: loadInfo.fileData, length: nullLength,
      messageOffset: loadInfo.messageOffset)
    let arrowOffsetBuffer = makeBuffer(
      offsetBuffer, fileData: loadInfo.fileData, length: UInt(node.length + 1),
      messageOffset: loadInfo.messageOffset)

    guard field.childrenCount == 1, let childField = field.children(at: 0) else {
      return .failure(.invalid("List must have exactly one child"))
    }

    switch loadField(loadInfo, field: childField) {
    case .success(let childHolder):
      return makeArrayHolder(
        field, buffers: [arrowNullBuffer, arrowOffsetBuffer], nullCount: UInt(node.nullCount),
        children: [childHolder.array.arrowData],
        rbLength: UInt(loadInfo.batchData.recordBatch.length))
    case .failure(let error):
      return .failure(error)
    }
  }

  private func loadPrimitiveData(
    _ loadInfo: DataLoadInfo,
    field: FlatField
  )
    -> Result<ArrowArrayHolder, ArrowError>
  {
    guard let node = loadInfo.batchData.nextNode() else {
      return .failure(.invalid("Node not found"))
    }

    guard let nullBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.invalid("Null buffer not found"))
    }

    guard let valueBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.invalid("Value buffer not found"))
    }

    let nullLength = UInt(ceil(Double(node.length) / 8))
    let arrowNullBuffer = makeBuffer(
      nullBuffer, fileData: loadInfo.fileData,
      length: nullLength, messageOffset: loadInfo.messageOffset)
    let arrowValueBuffer = makeBuffer(
      valueBuffer, fileData: loadInfo.fileData,
      length: UInt(node.length), messageOffset: loadInfo.messageOffset)
    return makeArrayHolder(
      field, buffers: [arrowNullBuffer, arrowValueBuffer],
      nullCount: UInt(node.nullCount), children: nil,
      rbLength: UInt(loadInfo.batchData.recordBatch.length))
  }

  private func loadVariableData(
    _ loadInfo: DataLoadInfo,
    field: FlatField
  )
    -> Result<ArrowArrayHolder, ArrowError>
  {
    guard let node = loadInfo.batchData.nextNode() else {
      return .failure(.invalid("Node not found"))
    }

    guard let nullBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.invalid("Null buffer not found"))
    }

    guard let offsetBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.invalid("Offset buffer not found"))
    }

    guard let valueBuffer = loadInfo.batchData.nextBuffer() else {
      return .failure(.invalid("Value buffer not found"))
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
    field: FlatField
  )
    -> Result<ArrowArrayHolder, ArrowError>
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
    _ recordBatch: FlatRecordBatch,
    schema: Schema,
    arrowSchema: ArrowSchema,
    data: Data,
    messageEndOffset: Int64
  ) -> Result<RecordBatch, ArrowError> {
    var columns: [ArrowArrayHolder] = []
    let batchData = RecordBatchData(recordBatch, schema: schema)
    let loadInfo = DataLoadInfo(
      fileData: data,
      messageOffset: messageEndOffset,
      batchData: batchData)
    while !batchData.isDone() {
      guard let field = batchData.nextField() else {
        return .failure(.invalid("Field not found"))
      }

      let result = loadField(loadInfo, field: field)
      switch result {
      case .success(let holder):
        columns.append(holder)
      case .failure(let error):
        return .failure(error)
      }
    }
    return .success(RecordBatch(arrowSchema, columns: columns))
  }

  /// This is for reading the Arrow streaming format. The Arrow streaming format
  /// is slightly different from the Arrow File format as it doesn't contain a header
  /// and footer.
  /// - Parameters:
  ///   - input: The buffer to read from
  /// - Returns: An `ArrowReaderResult` If successful, otherwise an `ArrowError`.
  public func readStreaming(
    _ input: Data,
    useUnalignedBuffers: Bool = false
  ) -> Result<ArrowReaderResult, ArrowError> {
    let result = ArrowReaderResult()
    var offset: Int = 0
    var length = getUInt32(input, offset: offset)
    var streamData = input
    var schemaMessage: Schema?
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
        data: streamData,
        allowReadingUnalignedBuffers: true
      )
      let message: Message = getRoot(byteBuffer: &dataBuffer)
      switch message.headerType {
      case .recordbatch:
        let rbMessage = message.header(type: FlatRecordBatch.self)!
        let recordBatchResult = loadRecordBatch(
          rbMessage,
          schema: schemaMessage!,
          arrowSchema: result.schema!,
          data: input,
          messageEndOffset: (Int64(offset) + Int64(length)))
        switch recordBatchResult {
        case .success(let recordBatch):
          result.batches.append(recordBatch)
        case .failure(let error):
          return .failure(error)
        }
        offset += Int(message.bodyLength + Int64(length))
        length = getUInt32(input, offset: offset)
      case .schema:
        schemaMessage = message.header(type: Schema.self)!
        let schemaResult = loadSchema(schemaMessage!)
        switch schemaResult {
        case .success(let schema):
          result.schema = schema
        case .failure(let error):
          return .failure(error)
        }
        offset += Int(message.bodyLength + Int64(length))
        length = getUInt32(input, offset: offset)
      default:
        return .failure(.unknownError("Unhandled header type: \(message.headerType)"))
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
      rawBuffer.loadUnaligned(fromByteOffset: fileData.count - 4, as: Int32.self)
    }

    let result = ArrowReaderResult()
    let footerStartOffset = fileData.count - Int(footerLength + 4)
    let footerData = fileData[footerStartOffset...]
    var footerBuffer = ByteBuffer(
      data: footerData,
      allowReadingUnalignedBuffers: useUnalignedBuffers)
    let footer: Footer = getRoot(byteBuffer: &footerBuffer)
    let schemaResult = loadSchema(footer.schema!)
    switch schemaResult {
    case .success(let schema):
      result.schema = schema
    case .failure(let error):
      return .failure(error)
    }

    for index in 0..<footer.recordBatchesCount {
      let recordBatch = footer.recordBatches(at: index)!
      var messageLength = fileData.withUnsafeBytes { rawBuffer in
        rawBuffer.loadUnaligned(fromByteOffset: Int(recordBatch.offset), as: UInt32.self)
      }

      var messageOffset: Int64 = 1
      if messageLength == continuationMarker {
        messageOffset += 1
        messageLength = fileData.withUnsafeBytes { rawBuffer in
          rawBuffer.loadUnaligned(
            fromByteOffset: Int(recordBatch.offset + Int64(MemoryLayout<Int32>.size)),
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
      let message: Message = getRoot(byteBuffer: &mbb)
      switch message.headerType {
      case .recordbatch:
        let rbMessage = message.header(type: FlatRecordBatch.self)!
        let recordBatchResult = loadRecordBatch(
          rbMessage,
          schema: footer.schema!,
          arrowSchema: result.schema!,
          data: fileData,
          messageEndOffset: messageEndOffset)
        switch recordBatchResult {
        case .success(let recordBatch):
          result.batches.append(recordBatch)
        case .failure(let error):
          return .failure(error)
        }
      default:
        return .failure(.unknownError("Unhandled header type: \(message.headerType)"))
      }
    }

    return .success(result)
  }

  public func fromFile(_ fileURL: URL) -> Result<ArrowReaderResult, ArrowError> {
    do {
      let fileData = try Data(contentsOf: fileURL)
      if !validateFileData(fileData) {
        return .failure(.ioError("Not a valid arrow file."))
      }
      let markerLength = fileMarker.utf8.count
      let footerLengthEnd = Int(fileData.count - markerLength)
      let data = fileData[..<(footerLengthEnd)]
      return readFile(data)
    } catch {
      return .failure(.unknownError("Error loading file: \(error)"))
    }
  }

  static public func makeArrowReaderResult() -> ArrowReaderResult {
    return ArrowReaderResult()
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
    let message: Message = getRoot(byteBuffer: &mbb)
    switch message.headerType {
    case .schema:
      let sMessage = message.header(type: Schema.self)!
      switch loadSchema(sMessage) {
      case .success(let schema):
        result.schema = schema
        result.messageSchema = sMessage
        return .success(())
      case .failure(let error):
        return .failure(error)
      }
    case .recordbatch:
      let rbMessage = message.header(type: FlatRecordBatch.self)!
      let recordBatchResult = loadRecordBatch(
        rbMessage, schema: result.messageSchema!, arrowSchema: result.schema!,
        data: dataBody, messageEndOffset: 0)
      switch recordBatchResult {
      case .success(let recordBatch):
        result.batches.append(recordBatch)
        return .success(())
      case .failure(let error):
        return .failure(error)
      }
    default:
      return .failure(.unknownError("Unhandled header type: \(message.headerType)"))
    }
  }
}
