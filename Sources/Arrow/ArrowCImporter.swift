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

import ArrowC
import Foundation

public class ArrowCImporter {
  private func appendToBuffer(
    _ cBuffer: UnsafeRawPointer?,
    arrowBuffers: inout [ArrowBuffer],
    length: UInt
  ) throws(ArrowError) {
    if cBuffer == nil {
      // Some implementations may have null buffers.
      // The empty buffers are positional placeholders.
      arrowBuffers.append(ArrowBuffer.createEmptyBuffer())
      return
    }
    guard let pointer = UnsafeMutableRawPointer(mutating: cBuffer) else {
      throw .init(.invalid("Failed to obtain a pointer to C buffer."))
    }
    arrowBuffers.append(
      ArrowBuffer(
        length: length,
        capacity: length,
        rawPointer: pointer,
        isMemoryOwner: false
      )
    )
  }

  public init() {}

  public func importType(_ cArrow: String, name: String = "") -> Result<
    ArrowField, ArrowError
  > {
    do {
      let type = try ArrowType.fromCDataFormatId(cArrow)
      return .success(ArrowField(name: name, dataType: type, isNullable: true))
    } catch {
      return .failure(
        .init(.invalid("Error occurred while attempting to import type: \(error)")))
    }
  }

  public func importField(_ cSchema: ArrowC.ArrowSchema) -> Result<
    ArrowField, ArrowError
  > {
    if cSchema.n_children > 0 {
      ArrowCImporter.release(cSchema)
      return .failure(.init(.invalid("Children currently not supported")))
    } else if cSchema.dictionary != nil {
      ArrowCImporter.release(cSchema)
      return .failure(.init(.invalid("Dictinoary types currently not supported")))
    }

    switch importType(
      String(cString: cSchema.format), name: String(cString: cSchema.name))
    {
    case .success(let field):
      ArrowCImporter.release(cSchema)
      return .success(field)
    case .failure(let err):
      ArrowCImporter.release(cSchema)
      return .failure(err)
    }
  }

  public func importArray(
    _ cArray: UnsafePointer<ArrowC.ArrowArray>,
    arrowType: ArrowType,
    isNullable: Bool = false
  ) -> Result<AnyArrowArray, ArrowError> {
    let arrowField = ArrowField(
      name: "", dataType: arrowType, isNullable: isNullable)
    return importArray(cArray, arrowField: arrowField)
  }

  public func importArray(
    _ cArrayPtr: UnsafePointer<ArrowC.ArrowArray>,
    arrowField: ArrowField
  ) -> Result<AnyArrowArray, ArrowError> {
    let cArray = cArrayPtr.pointee
    if cArray.null_count < 0 {
      ArrowCImporter.release(cArrayPtr)
      return .failure(.init(.invalid("Uncomputed null count is not supported")))
    } else if cArray.n_children > 0 {
      ArrowCImporter.release(cArrayPtr)
      return .failure(.init(.invalid("Children currently not supported")))
    } else if cArray.dictionary != nil {
      ArrowCImporter.release(cArrayPtr)
      return .failure(.init(.invalid("Dictionary types currently not supported")))
    } else if cArray.offset != 0 {
      ArrowCImporter.release(cArrayPtr)
      return .failure(
        .init(.invalid("Offset of 0 is required but found offset: \(cArray.offset)")))
    }

    let arrowType = arrowField.type
    let length = UInt(cArray.length)
    let nullCount = UInt(cArray.null_count)
    var arrowBuffers: [ArrowBuffer] = []

    if cArray.n_buffers > 0 {
      if cArray.buffers == nil {
        ArrowCImporter.release(cArrayPtr)
        return .failure(.init(.invalid("C array buffers is nil")))
      }

      do {
        if arrowType.isVariable {
          if cArray.n_buffers != 3 {
            ArrowCImporter.release(cArrayPtr)
            return .failure(
              .init(.invalid(
                "Variable buffer count expected 3 but found \(cArray.n_buffers)"
              )))
          }
          try appendToBuffer(
            cArray.buffers[0],
            arrowBuffers: &arrowBuffers,
            length: UInt(ceil(Double(length) / 8))
          )
          try appendToBuffer(
            cArray.buffers[1],
            arrowBuffers: &arrowBuffers,
            length: length
          )
          guard let buffer1 = cArray.buffers[1] else {
            return .failure(.init(.invalid("C array buffer is nil")))
          }
          let lastOffsetLength =
            buffer1
            .advanced(by: Int(length) * MemoryLayout<Int32>.stride)
            .load(as: Int32.self)
          try appendToBuffer(
            cArray.buffers[2],
            arrowBuffers: &arrowBuffers,
            length: UInt(lastOffsetLength)
          )
        } else {

          if cArray.n_buffers != 2 {
            ArrowCImporter.release(cArrayPtr)
            return .failure(
              .init(.invalid("Expected buffer count 2 but found \(cArray.n_buffers)")))
          }

          try appendToBuffer(
            cArray.buffers[0], arrowBuffers: &arrowBuffers,
            length: UInt(ceil(Double(length) / 8)))
          try appendToBuffer(
            cArray.buffers[1], arrowBuffers: &arrowBuffers, length: length)
        }
      } catch {
        return .failure(error)
      }
    }

    switch makeArrayHolder(
      arrowField, buffers: arrowBuffers,
      nullCount: nullCount, children: nil, rbLength: 0)
    {
    case .success(let holder):
      holder.setCArrayPtr(cArrayPtr)
      return .success(holder)
    case .failure(let err):
      ArrowCImporter.release(cArrayPtr)
      return .failure(err)
    }
  }

  public static func release(_ cArrayPtr: UnsafePointer<ArrowC.ArrowArray>) {
    if cArrayPtr.pointee.release != nil {
      let cSchemaMutablePtr = UnsafeMutablePointer<ArrowC.ArrowArray>(
        mutating: cArrayPtr
      )
      cArrayPtr.pointee.release(cSchemaMutablePtr)
    }
  }

  public static func release(_ cSchema: ArrowC.ArrowSchema) {
    if cSchema.release != nil {
      let cSchemaPtr = UnsafeMutablePointer<ArrowC.ArrowSchema>.allocate(
        capacity: 1)
      cSchemaPtr.initialize(to: cSchema)
      cSchema.release(cSchemaPtr)
    }
  }
}
