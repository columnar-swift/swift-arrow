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
import BinaryParsing
import FlatBuffers
import Foundation

let fileMarker: [UInt8] = .init(Data("ARROW1".utf8))
let continuationMarker = UInt32(0xFFFF_FFFF)

/// A view over `Data` which backs an Arrow buffer.
struct FileDataBuffer {
  let data: Data
  let range: Range<Int>
}

/// An Arrow buffer backed by file data.
internal protocol ArrowBufferIPC: ArrowBufferProtocol {
  var buffer: FileDataBuffer { get }
}

extension ArrowBufferIPC {
  public func withUnsafeBytes<R>(
    _ body: (UnsafeRawBufferPointer) throws -> R
  ) rethrows -> R {
    try buffer.data.withUnsafeBytes { dataPtr in
      let rangedPtr = UnsafeRawBufferPointer(
        rebasing: dataPtr[buffer.range]
      )
      return try body(rangedPtr)
    }
  }
}

/// A `Data` backed buffer for null bitmaps and boolean arrays.
struct NullBufferIPC: NullBuffer, ArrowBufferIPC {

  let buffer: FileDataBuffer
  var valueCount: Int
  let nullCount: Int

  var length: Int {
    buffer.range.count
  }

  func isSet(_ bit: Int) -> Bool {
    precondition(bit < valueCount, "Bit index \(bit) out of range")
    let byteIndex = bit / 8
    //    precondition(length > byteIndex, "Bit index \(bit) out of range")
    let offsetIndex = buffer.range.lowerBound + byteIndex
    let byte = self.buffer.data[offsetIndex]
    return byte & (1 << (bit % 8)) > 0
  }
}

/// A `Data` backed buffer for fixed-width types.
struct FixedWidthBufferIPC<Element>: FixedWidthBufferProtocol, ArrowBufferIPC
where
  Element: Numeric, Element: BitwiseCopyable
{
  typealias ElementType = Element
  let buffer: FileDataBuffer
  var length: Int { buffer.range.count }

  subscript(index: Int) -> Element {
    buffer.data.withUnsafeBytes { rawBuffer in
      let sub = rawBuffer[buffer.range]
      let span = Span<Element>(_unsafeBytes: sub)
      return span[index]
    }
  }
}

/// A `Data` backed buffer for variable-length types.
struct VariableLengthBufferIPC<Element: VariableLength>:
  VariableLengthBufferProtocol, ArrowBufferIPC
{
  typealias ElementType = Element
  let buffer: FileDataBuffer
  var length: Int { buffer.range.count }

  func loadVariable(
    at startIndex: Int,
    arrayLength: Int
  ) -> Element {
    precondition(startIndex + arrayLength <= self.length)
    return buffer.data.withUnsafeBytes { rawBuffer in
      let offsetStart = buffer.range.lowerBound + startIndex
      let offsetEnd = offsetStart + arrayLength
      let slice = rawBuffer[offsetStart..<offsetEnd]
      let uint8Buffer = slice.bindMemory(to: UInt8.self)
      return Element(uint8Buffer)
    }
  }
}

/// A reader for the Arrow file format.
///
/// The Arrow file format supports  random access. The Arrow file format contains a header and footer
/// around the Arrow streaming format.
public struct ArrowReader {

  let data: Data

  /// Create an `ArrowReader` from a URL.
  ///
  /// - Parameter url: the file to read from.
  /// - Throws: a ParsingError if the file could not be read.
  public init(url: URL) throws {

    self.data = try Data(contentsOf: url, options: .mappedIfSafe)

    try data.withParserSpan { input in
      let marker = try [UInt8](parsing: &input, byteCount: 6)
      guard marker == fileMarker else {
        throw ArrowError.invalid("Invalid Arrow file")
      }
    }
  }

  func read() throws -> (ArrowSchema, [RecordBatch]) {

    let footerData = try data.withParserSpan { input in
      let count = input.count
      try input.seek(toAbsoluteOffset: count - 10)

      let footerLength = try Int(parsingLittleEndian: &input, byteCount: 4)
      try input.seek(toAbsoluteOffset: count - 10 - footerLength)

      return try [UInt8](parsing: &input, byteCount: footerLength)
    }

    var footerBuffer = ByteBuffer(
      data: Data(footerData)
    )

    let footer: FFooter = getRoot(byteBuffer: &footerBuffer)

    guard let schema = footer.schema else {
      throw ArrowError.invalid("Missing schema in footer")
    }
    let arrowSchema = try loadSchema(schema: schema)
    var recordBatches: [RecordBatch] = []

    // MARK: Record batch parsing
    for index in 0..<footer.recordBatchesCount {
      guard let block: FBlock = footer.recordBatches(at: index) else {
        throw ArrowError.invalid("Missing record batch at index \(index)")
      }

      let (message, offset) = try data.withParserSpan { input in
        try input.seek(toAbsoluteOffset: block.offset)
        let marker = try UInt32(parsingLittleEndian: &input)
        if marker != continuationMarker {
          throw ArrowError.invalid("Missing continuation marker.")
        }
        let messageLength = try UInt32(parsingLittleEndian: &input)
        let data = try [UInt8](parsing: &input, byteCount: Int(messageLength))
        // TODO: Not zero-copy. Maybe new API fixes this.
        var mbb = ByteBuffer(data: Data(data))
        let message: FMessage = getRoot(byteBuffer: &mbb)
        let offset = Int64(input.startPosition)
        return (message, offset)
      }

      guard message.headerType == .recordbatch else {
        throw ArrowError.invalid(
          "Expected RecordBatch message, got: \(message.headerType).")
      }

      guard let rbMessage = message.header(type: FRecordBatch.self) else {
        throw ArrowError.invalid("Expected RecordBatch as message header")
      }
      guard let footerSchema = footer.schema else {
        throw ArrowError.invalid("Expected schema in footer")
      }

      // MARK: Load arrays
      var arrays: [AnyArrowArrayProtocol] = .init()
      var nodeIndex: Int32 = 0
      var bufferIndex: Int32 = 0

      for field in arrowSchema.fields {

        let array = try loadField(
          rbMessage: rbMessage,
          field: field,
          offset: offset,
          nodeIndex: &nodeIndex,
          bufferIndex: &bufferIndex
        )
        arrays.append(array)
      }

      let recordBatch = RecordBatch(schema: arrowSchema, columns: arrays)
      recordBatches.append(recordBatch)
    }

    return (arrowSchema, recordBatches)
  }

  func loadField(
    rbMessage: FRecordBatch,
    field: ArrowField,
    offset: Int64,
    nodeIndex: inout Int32,
    bufferIndex: inout Int32
  ) throws -> AnyArrowArrayProtocol {
    guard nodeIndex < rbMessage.nodesCount,
      let node = rbMessage.nodes(at: nodeIndex)
    else {
      throw ArrowError.invalid("Missing node at index \(nodeIndex)")
    }
    nodeIndex += 1
    let buffer0 = try nextBuffer(
      message: rbMessage,
      index: &bufferIndex,
      offset: offset,
      data: data
    )

    // MARK: Load arrays
    let nullCount = Int(node.nullCount)
    let length = Int(node.length)
    let nullsPresent = nullCount > 0
    let nullBuffer: NullBuffer
    if nullsPresent {
      if nullCount == 0 {
        nullBuffer = AllValidNullBuffer(valueCount: length)
      } else if length == 0 {
        nullBuffer = AllValidNullBuffer(valueCount: 0)
      } else if nullCount == length {
        nullBuffer = AllNullBuffer(valueCount: length)
      } else {
        nullBuffer = NullBufferIPC(
          buffer: buffer0, valueCount: length, nullCount: nullCount)
      }
    } else {
      nullBuffer = AllValidNullBuffer(valueCount: length)
    }

    if nullsPresent && !field.isNullable {
      print("Nullabity violated for field \(field.name)")
    }

    let arrowType = field.type
    if arrowType == .boolean {
      let buffer1 = try nextBuffer(
        message: rbMessage, index: &bufferIndex, offset: offset, data: data)
      let valueBuffer = NullBufferIPC(
        buffer: buffer1, valueCount: length, nullCount: nullCount)
      return ArrowArrayBoolean(
        offset: 0, length: length, nullBuffer: nullBuffer,
        valueBuffer: valueBuffer)
    } else if arrowType.isNumeric {
      let buffer1 = try nextBuffer(
        message: rbMessage, index: &bufferIndex, offset: offset, data: data)
      switch arrowType {
      case .float32:
        return makeFixedArray(
          length: length, elementType: Float.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .float64:
        return makeFixedArray(
          length: length, elementType: Double.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .int8:
        return makeFixedArray(
          length: length, elementType: Int8.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .uint8:
        return makeFixedArray(
          length: length, elementType: UInt8.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .int16:
        return makeFixedArray(
          length: length, elementType: Int16.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .uint16:
        return makeFixedArray(
          length: length, elementType: UInt16.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .int32:
        return makeFixedArray(
          length: length, elementType: Int32.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .uint32:
        return makeFixedArray(
          length: length, elementType: UInt32.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .int64:
        return makeFixedArray(
          length: length, elementType: Int64.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      case .uint64:
        return makeFixedArray(
          length: length, elementType: UInt64.self,
          nullBuffer: nullBuffer, buffer: buffer1)
      default:
        throw ArrowError.invalid("TODO: Unimplemented arrow type: \(arrowType)")
      }
    } else if arrowType.isVariable {
      let buffer1 = try nextBuffer(
        message: rbMessage, index: &bufferIndex, offset: offset, data: data)
      let buffer2 = try nextBuffer(
        message: rbMessage, index: &bufferIndex, offset: offset, data: data)

      if arrowType == .utf8 {
        return ArrowArrayVariable.utf8(
          length: length,
          nullBuffer: nullBuffer,
          offsetsBuffer: buffer1,
          valueBuffer: buffer2
        )
      } else if arrowType == .binary {
        return ArrowArrayVariable.binary(
          length: length,
          nullBuffer: nullBuffer,
          offsetsBuffer: buffer1,
          valueBuffer: buffer2
        )
      } else {
        throw ArrowError.notImplemented
      }
    } else if arrowType.isNested {
      switch arrowType {
      case .list(let field):
        let array: AnyArrowArrayProtocol = try loadField(
          rbMessage: rbMessage,
          field: field,
          offset: offset,
          nodeIndex: &nodeIndex,
          bufferIndex: &bufferIndex
        )
        let buffer1 = try nextBuffer(
          message: rbMessage, index: &bufferIndex, offset: offset, data: data)
        var offsetsBuffer = FixedWidthBufferIPC<Int32>(buffer: buffer1)

        // TODO: This is a hack for the special-case where buffer length 0 means all-zero offset.
        // Can follow the null buffer example.
        if offsetsBuffer.length != length + 1 {
          let offsetCount = length + 1
          let byteCount = offsetCount * MemoryLayout<Int32>.stride
          let fileDataBuffer = FileDataBuffer(
            data: Data(count: byteCount),  // Zero-initialized
            range: 0..<byteCount
          )
          offsetsBuffer = FixedWidthBufferIPC<Int32>(buffer: fileDataBuffer)
        }
        return makeListArray(
          length: length,
          nullBuffer: nullBuffer,
          offsetsBuffer: offsetsBuffer,
          values: array
        )
      case .strct(let fields):
        var arrays: [(String, AnyArrowArrayProtocol)] = []
        for field in fields {
          let array = try loadField(
            rbMessage: rbMessage,
            field: field,
            offset: offset,
            nodeIndex: &nodeIndex,
            bufferIndex: &bufferIndex
          )
          arrays.append((field.name, array))
        }
        return ArrowStructArray(
          length: length,
          nullBuffer: nullBuffer,
          fields: arrays
        )
      default:
        throw ArrowError.notImplemented
      }
    } else {
      // MARK: Unclassifiable types.
      if case .fixedSizeBinary(let byteWidth) = arrowType {
        let valueBuffer = try nextBuffer(
          message: rbMessage, index: &bufferIndex, offset: offset, data: data)
        let valueBufferTyped = VariableLengthBufferIPC<Data>(
          buffer: valueBuffer)
        return ArrowArrayFixedSizeBinary(
          length: length,
          byteWidth: Int(byteWidth),
          nullBuffer: nullBuffer,
          valueBuffer: valueBufferTyped
        )
      }
      throw ArrowError.notImplemented
    }
  }

  func nextBuffer(
    message: FRecordBatch, index: inout Int32, offset: Int64, data: Data
  ) throws -> FileDataBuffer {
    guard index < message.buffersCount, let buffer = message.buffers(at: index)
    else {
      throw ArrowError.invalid("Invalid buffer index.")
    }
    index += 1
    let startOffset = offset + buffer.offset
    let endOffset = startOffset + buffer.length
    let range = Int(startOffset)..<Int(endOffset)
    let fileDataBuffer = FileDataBuffer(data: data, range: range)
    return fileDataBuffer
  }

  func makeFixedArray<T>(
    length: Int,
    elementType: T.Type,
    nullBuffer: NullBuffer,
    buffer: FileDataBuffer
  ) -> ArrowArrayFixed<FixedWidthBufferIPC<T>> {
    let fixedBuffer = FixedWidthBufferIPC<T>(buffer: buffer)
    return ArrowArrayFixed(
      length: length,
      nullBuffer: nullBuffer,
      valueBuffer: fixedBuffer
    )
  }

  func makeListArray<Element>(
    length: Int,
    nullBuffer: NullBuffer,
    offsetsBuffer: FixedWidthBufferIPC<Int32>,
    values: Element
  ) -> AnyArrowListArray where Element: AnyArrowArrayProtocol {
    let list = ArrowListArray(
      length: length,
      nullBuffer: nullBuffer,
      offsetsBuffer: offsetsBuffer,
      values: values
    )
    return AnyArrowListArray(list)
  }

  private func loadSchema(schema: FSchema) throws(ArrowError) -> ArrowSchema {
    let metadata = (0..<schema.customMetadataCount)
      .reduce(into: [String: String]()) { dict, index in
        guard let customMetadata = schema.customMetadata(at: index),
          let key = customMetadata.key
        else { return }
        dict[key] = customMetadata.value
      }
    var fields: [ArrowField] = []
    for index in 0..<schema.fieldsCount {
      guard let field = schema.fields(at: index) else {
        throw .invalid("Field not found at index: \(index)")
      }
      let fieldType: ArrowType = try .type(for: field)
      guard let fieldName = field.name else {
        throw .invalid("Field name not found")
      }
      let fieldMetadata = (0..<field.customMetadataCount)
        .reduce(into: [String: String]()) { dict, index in
          guard let customMetadata = field.customMetadata(at: index),
            let key = customMetadata.key
          else { return }
          dict[key] = customMetadata.value
        }
      let arrowField = ArrowField(
        name: fieldName,
        dataType: fieldType,
        isNullable: field.nullable,
        metadata: fieldMetadata
      )
      fields.append(arrowField)
    }
    return ArrowSchema(fields, metadata: metadata)
  }

}
