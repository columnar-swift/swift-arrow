// Copyright 2025 The Apache Software Foundation
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
import XCTest

@testable import Arrow

final class CDataTests: XCTestCase {
  func makeSchema() -> Arrow.ArrowSchema {
    let schemaBuilder = ArrowSchema.Builder()
    return
      schemaBuilder
      .addField("colBool", type: ArrowType(ArrowType.arrowBool), isNullable: false)
      .addField("colUInt8", type: ArrowType(ArrowType.arrowUInt8), isNullable: true)
      .addField("colUInt16", type: ArrowType(ArrowType.arrowUInt16), isNullable: true)
      .addField("colUInt32", type: ArrowType(ArrowType.arrowUInt32), isNullable: true)
      .addField("colUInt64", type: ArrowType(ArrowType.arrowUInt64), isNullable: true)
      .addField("colInt8", type: ArrowType(ArrowType.arrowInt8), isNullable: false)
      .addField("colInt16", type: ArrowType(ArrowType.arrowInt16), isNullable: false)
      .addField("colInt32", type: ArrowType(ArrowType.arrowInt32), isNullable: false)
      .addField("colInt64", type: ArrowType(ArrowType.arrowInt64), isNullable: false)
      .addField("colString", type: ArrowType(ArrowType.arrowString), isNullable: false)
      .addField("colBinary", type: ArrowType(ArrowType.arrowBinary), isNullable: false)
      .addField("colDate32", type: ArrowType(ArrowType.arrowDate32), isNullable: false)
      .addField("colDate64", type: ArrowType(ArrowType.arrowDate64), isNullable: false)
      .addField("colTime32", type: ArrowType(ArrowType.arrowTime32), isNullable: false)
      .addField("colTime32s", type: ArrowTypeTime32(.seconds), isNullable: false)
      .addField("colTime32m", type: ArrowTypeTime32(.milliseconds), isNullable: false)
      .addField("colTime64", type: ArrowType(ArrowType.arrowTime64), isNullable: false)
      .addField("colTime64u", type: ArrowTypeTime64(.microseconds), isNullable: false)
      .addField("colTime64n", type: ArrowTypeTime64(.nanoseconds), isNullable: false)
      .addField("colTimestamp", type: ArrowType(ArrowType.arrowTimestamp), isNullable: false)
      .addField("colTimestampts", type: ArrowTypeTimestamp(.seconds), isNullable: false)
      .addField("colTimestamptm", type: ArrowTypeTimestamp(.milliseconds), isNullable: false)
      .addField("colTimestamptu", type: ArrowTypeTimestamp(.microseconds), isNullable: false)
      .addField("colTimestamptn", type: ArrowTypeTimestamp(.nanoseconds), isNullable: false)
      .addField("colFloat", type: ArrowType(ArrowType.arrowFloat), isNullable: false)
      .addField("colDouble", type: ArrowType(ArrowType.arrowDouble), isNullable: false)
      .finish()
  }

  func checkImportField(_ cSchema: ArrowC.ArrowSchema, name: String, type: ArrowType.Info) throws {
    let importer = ArrowCImporter()
    switch importer.importField(cSchema) {
    case .success(let arrowField):
      XCTAssertEqual(arrowField.type.info, type)
      XCTAssertEqual(arrowField.name, name)
    case .failure(let error):
      throw error
    }
  }

  #if os(macOS) || os(iOS) || os(tvOS) || os(watchOS)
  @MainActor
  func testImportExportSchema() throws {
    let schema = makeSchema()
    let exporter = ArrowCExporter()
    for arrowField in schema.fields {
      var cSchema = ArrowC.ArrowSchema()
      switch exporter.exportField(&cSchema, field: arrowField) {
      case .success:
        try checkImportField(cSchema, name: arrowField.name, type: arrowField.type.info)
      case .failure(let error):
        throw error
      }
    }
  }

  @MainActor
  func testImportExportArray() throws {
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    for index in 0..<100 {
      if index % 10 == 9 {
        stringBuilder.append(nil)
      } else {
        stringBuilder.append("test" + String(index))
      }
    }

    XCTAssertEqual(stringBuilder.nullCount, 10)
    XCTAssertEqual(stringBuilder.length, 100)
    XCTAssertEqual(stringBuilder.capacity, 648)
    let stringArray = try stringBuilder.finish()
    let exporter = ArrowCExporter()
    var cArray = ArrowC.ArrowArray()
    exporter.exportArray(&cArray, arrowData: stringArray.arrowData)
    let cArrayMutPtr = UnsafeMutablePointer<ArrowC.ArrowArray>.allocate(capacity: 1)
    cArrayMutPtr.pointee = cArray
    defer {
      cArrayMutPtr.deallocate()
    }

    let importer = ArrowCImporter()
    switch importer.importArray(
      UnsafePointer(cArrayMutPtr), arrowType: ArrowType(ArrowType.arrowString))
    {
    case .success(let holder):
      let builder = RecordBatch.Builder()
      switch builder
        .addColumn("test", arrowArray: holder)
        .finish()
      {
      case .success(let rb):
        XCTAssertEqual(rb.columnCount, 1)
        XCTAssertEqual(rb.length, 100)
        let col1: Arrow.ArrowArray<String> = rb.data(for: 0)
        for index in 0..<col1.length {
          if index % 10 == 9 {
            XCTAssertEqual(col1[index], nil)
          } else {
            XCTAssertEqual(col1[index], "test" + String(index))
          }
        }
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }
  #endif
}
