// CDataTests.swift
// Arrow
//
// Created by Will Temperley on 04/11/2025. All rights reserved.
// Copyright 2025 Will Temperley.
//
// Copying or reproduction of this file via any medium requires prior express
// written permission from the copyright holder.
// -----------------------------------------------------------------------------
///
/// Implementation notes, links and internal documentation go here.
///
// -----------------------------------------------------------------------------

import ArrowC
import Testing
@testable import Arrow

struct CDataTests {

  func makeSchema() -> Arrow.ArrowSchema {
    let schemaBuilder = ArrowSchema.Builder()
    return
      schemaBuilder
      .addField("colBool", type: .boolean, isNullable: false)
      .addField("colUInt8", type: .uint8, isNullable: true)
      .addField("colUInt16", type: .uint16, isNullable: true)
      .addField("colUInt32", type: .uint32, isNullable: true)
      .addField("colUInt64", type: .uint64, isNullable: true)
      .addField("colInt8", type: .int8, isNullable: false)
      .addField("colInt16", type: .int16, isNullable: false)
      .addField("colInt32", type: .int32, isNullable: false)
      .addField("colInt64", type: .int64, isNullable: false)
      .addField("colString", type: .utf8, isNullable: false)
      .addField("colBinary", type: .binary, isNullable: false)
      .addField("colDate32", type: .date32, isNullable: false)
      .addField("colDate64", type: .date64, isNullable: false)
      //        .addField("colTime32", type: .time32, isNullable: false)
      .addField("colTime32s", type: .time32(.second), isNullable: false)
      .addField("colTime32m", type: .time32(.millisecond), isNullable: false)
      //                 .addField("colTime64", type: .time64, isNullable: false)
      .addField("colTime64u", type: .time64(.microsecond), isNullable: false)
      .addField("colTime64n", type: .time64(.nanosecond), isNullable: false)
      //            .addField("colTimestamp", type: ArrowType(ArrowType.ArrowTimestamp), isNullable: false)
      .addField(
        "colTimestampts", type: .timestamp(.second, nil), isNullable: false
      )
      .addField(
        "colTimestamptm", type: .timestamp(.millisecond, nil), isNullable: false
      )
      .addField(
        "colTimestamptu", type: .timestamp(.microsecond, nil), isNullable: false
      )
      .addField(
        "colTimestamptn", type: .timestamp(.nanosecond, nil), isNullable: false
      )
      .addField("colFloat", type: .float32, isNullable: false)
      .addField("colDouble", type: .float64, isNullable: false)
      .finish()
  }

  func checkImportField(
    _ cSchema: ArrowC.ArrowSchema, name: String, type: ArrowType
  ) throws {
    let importer = ArrowCImporter()
    switch importer.importField(cSchema) {
    case .success(let arrowField):
      #expect(arrowField.type == type)
      #expect(arrowField.name == name)
    case .failure(let error):
      throw error
    }
  }

  @Test
  @MainActor
  func testImportExportSchema() throws {
    let schema = makeSchema()
    let exporter = ArrowCExporter()
    for arrowField in schema.fields {
      var cSchema = ArrowC.ArrowSchema()
      switch exporter.exportField(&cSchema, field: arrowField) {
      case .success:
        try checkImportField(
          cSchema, name: arrowField.name, type: arrowField.type)
      case .failure(let error):
        throw error
      }
    }
  }

  @Test
  @MainActor
  func testImportExportArray() throws {
//    Issue.record("Fix the deallocation. Test it too if possible.")
    let stringBuilder = try ArrowArrayBuilders.loadStringArrayBuilder()
    for index in 0..<100 {
      if index % 10 == 9 {
        stringBuilder.append(nil)
      } else {
        stringBuilder.append("test" + String(index))
      }
    }

    #expect(stringBuilder.nullCount == 10)
    #expect(stringBuilder.length == 100)
    //        XCTAssertEqual(stringBuilder.capacity, 640)
    let stringArray = try stringBuilder.finish()
    let exporter = ArrowCExporter()
    var cArray = ArrowC.ArrowArray()
    exporter.exportArray(&cArray, arrowData: stringArray.arrowData)
    let cArrayMutPtr = UnsafeMutablePointer<ArrowC.ArrowArray>.allocate(
      capacity: 1)
    cArrayMutPtr.pointee = cArray
    defer {
      cArrayMutPtr.deallocate()
    }

    let importer = ArrowCImporter()
    switch importer.importArray(UnsafePointer(cArrayMutPtr), arrowType: .utf8) {
    case .success(let holder):
      let builder = RecordBatch.Builder()
      switch builder
        .addColumn("test", arrowArray: holder)
        .finish()
      {
      case .success(let rb):
        #expect(rb.columnCount == 1)
        #expect(rb.length == 100)
        let col1: any Arrow.ArrowArray<String> = try rb.data(for: 0)
        for index in 0..<col1.length {
          if index % 10 == 9 {
            #expect(col1[index] == nil)
          } else {
            #expect(col1[index] == "test" + String(index))
          }
        }
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }
}
