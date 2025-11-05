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
import Atomics
import Foundation

// The memory used by UnsafeAtomic is not automatically
// reclaimed. Since this value is initialized once
// and used until the program/app is closed it's
// memory will be released on program/app exit
let exportDataCounter: UnsafeAtomic<Int> = .create(0)

public class ArrowCExporter {
  private class ExportData {
    let id: Int
    @MainActor
    init() {
      id = exportDataCounter.loadThenWrappingIncrement(ordering: .relaxed)
      ArrowCExporter.exportedData[id] = self
    }
  }

  private class ExportSchema: ExportData {
    public let arrowTypeNameCstr: UnsafePointer<CChar>
    public let nameCstr: UnsafePointer<CChar>
    private let arrowType: ArrowType
    private let name: String
    private let arrowTypeName: String
    @MainActor
    init(_ arrowType: ArrowType, name: String = "") throws {
      self.arrowType = arrowType
      // keeping the name str to ensure the cstring buffer remains valid
      self.name = name
      self.arrowTypeName = try arrowType.cDataFormatId
      guard let nameCstr = (self.name as NSString).utf8String else {
        throw ArrowError.runtimeError("Failed to convert name to C string")
      }
      self.nameCstr = nameCstr
      guard let typeNameCstr = (self.arrowTypeName as NSString).utf8String
      else {
        throw ArrowError.runtimeError("Failed to convert type name to C string")
      }
      self.arrowTypeNameCstr = typeNameCstr
      super.init()
    }
  }

  private class ExportArray: ExportData {
    private let arrowData: ArrowData
    private(set) var data: [UnsafeRawPointer?] = []
    private(set) var buffers: UnsafeMutablePointer<UnsafeRawPointer?>
    @MainActor
    init(_ arrowData: ArrowData) {
      // keep a reference to the ArrowData
      // obj so the memory doesn't get
      // deallocated
      self.arrowData = arrowData
      for arrowBuffer in arrowData.buffers {
        self.data.append(arrowBuffer.rawPointer)
      }

      self.buffers = UnsafeMutablePointer<UnsafeRawPointer?>.allocate(
        capacity: self.data.count)
      self.buffers.initialize(from: &self.data, count: self.data.count)
      super.init()
    }

    deinit {
      self.buffers.deinitialize(count: self.data.count)
      self.buffers.deallocate()
    }
  }

  @MainActor private static var exportedData: [Int: ExportData] = [:]
  public init() {}

  @MainActor public func exportType(
    _ cSchema: inout ArrowC.ArrowSchema, arrowType: ArrowType, name: String = ""
  ) -> Result<Bool, ArrowError> {
    do {
      let exportSchema = try ExportSchema(arrowType, name: name)
      cSchema.format = exportSchema.arrowTypeNameCstr
      cSchema.name = exportSchema.nameCstr
      cSchema.private_data =
        UnsafeMutableRawPointer(
          mutating: UnsafeRawPointer(bitPattern: exportSchema.id))
      cSchema.release = { (data: UnsafeMutablePointer<ArrowC.ArrowSchema>?) in
        guard let data else {
          fatalError("Release called with nil Arrow schema pointer.")
        }
        let arraySchema = data.pointee
        let exportId = Int(bitPattern: arraySchema.private_data)
        guard ArrowCExporter.exportedData[exportId] != nil else {
          fatalError("Export schema not found with id \(exportId)")
        }

        // the data associated with this exportSchema object
        // which includes the C strings for the format and name
        // be deallocated upon removal
        ArrowCExporter.exportedData.removeValue(forKey: exportId)
        ArrowC.ArrowSwiftClearReleaseSchema(data)
      }
    } catch {
      return .failure(.unknownError("\(error)"))
    }
    return .success(true)
  }

  @MainActor public func exportField(
    _ schema: inout ArrowC.ArrowSchema, field: ArrowField
  ) -> Result<Bool, ArrowError> {
    exportType(&schema, arrowType: field.type, name: field.name)
  }

  @MainActor public func exportArray(
    _ cArray: inout ArrowC.ArrowArray, arrowData: ArrowData
  ) {
    let exportArray = ExportArray(arrowData)
    cArray.buffers = exportArray.buffers
    cArray.length = Int64(arrowData.length)
    cArray.null_count = Int64(arrowData.nullCount)
    cArray.n_buffers = Int64(arrowData.buffers.count)
    // Swift Arrow does not currently support children or dictionaries
    // This will need to be updated once support has been added
    cArray.n_children = 0
    cArray.children = nil
    cArray.dictionary = nil
    cArray.private_data = UnsafeMutableRawPointer(
        mutating: UnsafeRawPointer(bitPattern: exportArray.id)
    )
    cArray.release = { (data: UnsafeMutablePointer<ArrowC.ArrowArray>?) in
      guard let data else {
        fatalError("Release called with nil ArrowArray pointer.")
      }
      let arrayData = data.pointee
      let exportId = Int(bitPattern: arrayData.private_data)
      guard ArrowCExporter.exportedData[exportId] != nil else {
        fatalError("Export data not found with id \(exportId)")
      }

      // the data associated with this exportArray object
      // which includes the entire arrowData object
      // and the buffers UnsafeMutablePointer[] will
      // be deallocated upon removal
      ArrowCExporter.exportedData.removeValue(forKey: exportId)
      ArrowC.ArrowSwiftClearReleaseArray(data)
    }
  }
}
