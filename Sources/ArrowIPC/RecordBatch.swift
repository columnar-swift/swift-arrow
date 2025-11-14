// RecordBatch.swift
// Arrow
//
// Created by Will Temperley on 14/11/2025. All rights reserved.
// Copyright 2025 Will Temperley.
//
// Copying or reproduction of this file via any medium requires prior express
// written permission from the copyright holder.
// -----------------------------------------------------------------------------
///
/// Implementation notes, links and internal documentation go here.
///
// -----------------------------------------------------------------------------

import Arrow

struct RecordBatch {

  let schema: ArrowSchema
  var columnCount: Int { Int(self.columns.count) }
  let columns: [any ArrowArrayProtocol]
  let length: Int

  public init(_ schema: ArrowSchema, columns: [any ArrowArrayProtocol]) {
    self.schema = schema
    self.columns = columns
    self.length = columns[0].length
  }

}
