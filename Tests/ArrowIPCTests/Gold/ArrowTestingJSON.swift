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
import Testing

@testable import Arrow
@testable import ArrowIPC

/// Tests round trip from JSON -> Array -> JSON.
///
/// See https://arrow.apache.org/docs/format/Integration.html#strategy
///
/// The producer typically reads a JSON file, converts it to in-memory Arrow data, and exposes this data
/// using the format under test. The consumer reads the data in the said format and converts it back to
/// Arrow in-memory data; it also reads the same JSON file as the producer, and validates that both
/// datasets are identical.
struct ArrowTestingJSON {

  static let testCases: [String] = [
    "generated_primitive",
    "generated_primitive_no_batches",
    "generated_primitive_zerolength",
    "generated_binary",
    "generated_binary_zerolength",
    "generated_custom_metadata",
    "generated_nested",
    "generated_recursive_nested",
  ]

  @Test(arguments: testCases)
  func json(name: String) throws {
    let resourceURL = try loadTestResource(
      name: name,
      withExtension: "json.lz4",
      subdirectory: "integration/cpp-21.0.0"
    )
    let lz4Data = try Data(contentsOf: resourceURL)
    let lz4 = try LZ4(parsing: lz4Data)
    let testCase = try JSONDecoder().decode(ArrowGold.self, from: lz4.data)
    let testFile = try loadTestResource(
      name: name,
      withExtension: "arrow_file",
      subdirectory: "integration/cpp-21.0.0"
    )
    let arrowReader = try ArrowReader(url: testFile)
    let (arrowSchema, recordBatches) = try arrowReader.read()
    #expect(testCase.batches.count == recordBatches.count)
    // Strip placeholder values.
    let expectedBatches = testCase.batches.map { batch in
      ArrowGold.Batch(
        count: batch.count,
        columns: batch.columns.map { $0.withoutJunkData() }
      )
    }
    let expectedSchema = testCase.schema
    let expectedDictionaries = testCase.dictionaries
    let expectedGold = ArrowGold(
      schema: expectedSchema,
      batches: expectedBatches,
      dictionaries: expectedDictionaries
    )
    let actualSchema = encode(schema: arrowSchema)
    #expect(actualSchema == expectedSchema)
    let actualBatches = try encode(batches: recordBatches, schema: arrowSchema)
    #expect(actualBatches == expectedBatches)
    let actualGold = ArrowGold(
      schema: actualSchema,
      batches: actualBatches,
      dictionaries: nil
    )
    #expect(actualGold == expectedGold)
  }
}

private func encode(schema: ArrowSchema) -> ArrowGold.Schema {
  let fields = schema.fields.map { arrowField in
    arrowField.toGoldField()
  }
  let encodedMetadata: [String: String]? =
    switch schema.metadata {
    case .none: nil
    case .some(let metadata): metadata.isEmpty ? nil : metadata
    }
  return .init(fields: fields, metadata: encodedMetadata)
}

private func encode(
  batches: [RecordBatch],
  schema: ArrowSchema
) throws -> [ArrowGold.Batch] {
  return try batches.map { recordBatch in
    var columns: [ArrowGold.Column] = []
    for (field, array) in zip(schema.fields, recordBatch.arrays) {
      let encoded = try encodeColumn(array: array, field: field)
      columns.append(encoded)
    }
    return .init(count: recordBatch.length, columns: columns)
  }
}
