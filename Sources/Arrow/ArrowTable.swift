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

import Foundation

public class ArrowColumn {
  let dataHolder: ChunkedArrayHolder
  public let field: ArrowField
  public var type: ArrowType { self.dataHolder.type }
  public var length: UInt { self.dataHolder.length }
  public var nullCount: UInt { self.dataHolder.nullCount }

  public var name: String { field.name }
  public init(_ field: ArrowField, chunked: ChunkedArrayHolder) {
    self.field = field
    self.dataHolder = chunked
  }

  public func data<T>() throws(ArrowError) -> ChunkedArray<T> {
    if let holder = self.dataHolder.holder as? ChunkedArray<T> {
      return holder
    } else {
      throw .init(
        .runtimeError("Could not cast array holder to chunked array."))
    }
  }
}

public class ArrowTable {
  public let schema: ArrowSchema
  public var columnCount: UInt { UInt(self.columns.count) }
  public let rowCount: UInt
  public let columns: [ArrowColumn]
  public init(_ schema: ArrowSchema, columns: [ArrowColumn]) {
    self.schema = schema
    self.columns = columns
    self.rowCount = columns[0].length
  }

  /// Create an ArrowTable from a 'RecordBatch' list.
  /// - Parameter recordBatches: The record batches.
  /// - Returns: An `ArrowResult` holding an `ArrowTable` on success, or an`ArrowError`
  ///   on failure.
  public static func from(
    recordBatches: [RecordBatchX]
  ) -> Result<ArrowTable, ArrowError> {
    if recordBatches.isEmpty {
      return .failure(.init(.arrayHasNoElements))
    }
    var holders: [[AnyArrowArray]] = []
    let schema = recordBatches[0].schema
    for recordBatch in recordBatches {
      for index in 0..<schema.fields.count {
        if holders.count <= index {
          holders.append([AnyArrowArray]())
        }
        holders[index].append(recordBatch.columns[index])
      }
    }
    let builder = ArrowTable.Builder()
    for index in 0..<schema.fields.count {
      do {
        let column = try makeArrowColumn(
          for: schema.fields[index],
          holders: holders[index]
        )
        builder.addColumn(column)
      } catch {
        return .failure(error)
      }
    }
    return .success(builder.finish())
  }

  private static func makeArrowColumn(
    for field: ArrowField,
    holders: [AnyArrowArray]
  ) throws(ArrowError) -> ArrowColumn {
    // Dispatch based on the field's type, not the first holder
    switch field.type {
    case .int8:
      return try makeTypedColumn(field, holders, type: Int8.self)
    case .int16:
      return try makeTypedColumn(field, holders, type: Int16.self)
    case .int32:
      return try makeTypedColumn(field, holders, type: Int32.self)
    case .int64:
      return try makeTypedColumn(field, holders, type: Int64.self)
    case .uint8:
      return try makeTypedColumn(field, holders, type: UInt8.self)
    case .uint16:
      return try makeTypedColumn(field, holders, type: UInt16.self)
    case .uint32:
      return try makeTypedColumn(field, holders, type: UInt32.self)
    case .uint64:
      return try makeTypedColumn(field, holders, type: UInt64.self)
    case .float32:
      return try makeTypedColumn(field, holders, type: Float.self)
    case .float64:
      return try makeTypedColumn(field, holders, type: Double.self)
    case .utf8, .binary:
      return try makeTypedColumn(field, holders, type: String.self)
    case .boolean:
      return try makeTypedColumn(field, holders, type: Bool.self)
    case .date32, .date64:
      return try makeTypedColumn(field, holders, type: Date.self)
    // TODO: make a fuzzer to make sure all types are hit
    default:
      throw .init(.unknownType("Unsupported type: \(field.type)"))
    }
  }

  private static func makeTypedColumn<T>(
    _ field: ArrowField,
    _ holders: [AnyArrowArray],
    type: T.Type
  ) throws(ArrowError) -> ArrowColumn {
    var arrays: [any ArrowArray<T>] = []
    for holder in holders {
      guard let array = holder as? (any ArrowArray<T>) else {
        throw .init(
          .runtimeError(
            "Array type mismatch: expected \(T.self) for field \(field.name)"
          ))
      }
      arrays.append(array)
    }
    return ArrowColumn(
      field,
      chunked: ChunkedArrayHolder(try ChunkedArray<T>(arrays))
    )
  }

  public class Builder {
    let schemaBuilder = ArrowSchema.Builder()
    var columns: [ArrowColumn] = []

    public init() {}

    @discardableResult
    public func addColumn<T>(
      _ fieldName: String,
      arrowArray: any ArrowArray<T>
    ) throws -> Builder {
      self.addColumn(fieldName, chunked: try ChunkedArray([arrowArray]))
    }

    @discardableResult
    public func addColumn<T>(
      _ fieldName: String,
      chunked: ChunkedArray<T>
    ) -> Builder {
      let field = ArrowField(
        name: fieldName,
        dataType: chunked.type,
        isNullable: chunked.nullCount != 0
      )
      self.schemaBuilder.addField(field)
      let column = ArrowColumn(field, chunked: ChunkedArrayHolder(chunked))
      self.columns.append(column)
      return self
    }

    @discardableResult
    public func addColumn<T>(
      _ field: ArrowField,
      arrowArray: any ArrowArray<T>
    ) throws -> Builder {
      self.schemaBuilder.addField(field)
      let holder = ChunkedArrayHolder(try ChunkedArray([arrowArray]))
      self.columns.append(ArrowColumn(field, chunked: holder))
      return self
    }

    @discardableResult
    public func addColumn<T>(
      _ field: ArrowField,
      chunked: ChunkedArray<T>
    ) -> Builder {
      self.schemaBuilder.addField(field)
      let column = ArrowColumn(field, chunked: ChunkedArrayHolder(chunked))
      self.columns.append(column)
      return self
    }

    @discardableResult
    public func addColumn(_ column: ArrowColumn) -> Builder {
      self.schemaBuilder.addField(column.field)
      self.columns.append(column)
      return self
    }

    public func finish() -> ArrowTable {
      ArrowTable(self.schemaBuilder.finish(), columns: self.columns)
    }
  }
}

public class RecordBatchX {
  public let schema: ArrowSchema
  public var columnCount: UInt { UInt(self.columns.count) }
  public let columns: [AnyArrowArray]
  public let length: UInt

  public init(_ schema: ArrowSchema, columns: [AnyArrowArray]) {
    self.schema = schema
    self.columns = columns
    self.length = columns[0].length
  }

  public class Builder {
    let schemaBuilder = ArrowSchema.Builder()
    var columns: [AnyArrowArray] = []

    public init() {}

    /// Add a column the `RecordBatch` builder.
    /// - Parameters:
    ///   - fieldName: The field name.
    ///   - arrowArray: The array to add to the reocrd batch.
    /// - Returns: The `RecordBatch.Builder` with the array appended and the field added to
    /// the schema. If the array contains zero nulls, the field is defined as non-null.
    @discardableResult
    public func addColumn(
      _ fieldName: String,
      arrowArray: AnyArrowArray
    ) -> Builder {
      let field = ArrowField(
        name: fieldName,
        dataType: arrowArray.type,
        isNullable: arrowArray.nullCount != 0
      )
      self.schemaBuilder.addField(field)
      self.columns.append(arrowArray)
      return self
    }

    /// Add a column the `RecordBatch` builder.
    /// - Parameters:
    ///   - field: The field describing the array.
    ///   - arrowArray: The array to add to the reocrd batch.
    /// - Returns: The `RecordBatch.Builder` with the array appended and the field added to
    /// the schema.
    @discardableResult
    public func addColumn(
      _ field: ArrowField,
      arrowArray: AnyArrowArray
    ) -> Builder {
      self.schemaBuilder.addField(field)
      self.columns.append(arrowArray)
      return self
    }

    public func finish() -> Result<RecordBatchX, ArrowError> {
      if columns.count > 0 {
        let columnLength = columns[0].length
        for column in columns {
          if column.length != columnLength {
            return .failure(
              .init(.runtimeError("Columns have different sizes")))
          }
        }
      }
      // Check nullability matches actual data
      let schema = self.schemaBuilder.finish()
      for (index, field) in schema.fields.enumerated() {
        let column = columns[index]
        if !field.isNullable && column.nullCount > 0 {
          return .failure(
            .init(
              .invalid(
                "non-nullable column '\(field.name)' contains \(column.nullCount) null values."
              )))

        }
      }
      return .success(
        RecordBatchX(self.schemaBuilder.finish(), columns: self.columns)
      )
    }
  }

  public func data<T>(
    for columnIndex: Int
  ) throws(ArrowError) -> any ArrowArray<T> {
    let arrayHolder = column(columnIndex)
    if let array = arrayHolder as? any ArrowArray<T> {
      return array
    } else {
      throw .init(
        .invalid(
          "Could not convert \(arrayHolder) for \(columnIndex)"
        ))
    }
  }

  public func column(_ index: Int) -> AnyArrowArray {
    self.columns[index]
  }

  public func column(_ name: String) -> AnyArrowArray? {
    if let index = self.schema.fieldIndex(name) {
      return self.columns[index]
    } else {
      return nil
    }
  }
}
