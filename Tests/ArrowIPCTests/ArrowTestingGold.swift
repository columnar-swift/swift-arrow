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
struct ArrowTestingGold {

  static let testCases: [String] = [
    "generated_primitive",
    "generated_primitive_no_batches",
    "generated_primitive_zerolength",
    "generated_binary",
    "generated_binary_zerolength",
    "generated_custom_metadata",
    "generated_nested",
    "generated_recursive_nested",
    "generated_map",
    "generated_datetime",
    "generated_duration",
  ]

  @Test(arguments: testCases)
  func read(name: String) throws {
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
    // These comparisons are redundant but help pinpoint where issues arise.
    let actualSchema = encode(schema: arrowSchema)
    #expect(actualSchema == expectedSchema)
    let actualBatches = try encode(batches: recordBatches, schema: arrowSchema)
    #expect(actualBatches.count == expectedBatches.count)
    #expect(actualBatches == expectedBatches)
    if actualBatches != expectedBatches {
      try printCodable(actualBatches)
      try diffEncodable(actualBatches, expectedBatches)
    }
    let actualGold = ArrowGold(
      schema: actualSchema,
      batches: actualBatches,
      dictionaries: nil
    )
    // The gold-standard comparison.
    #expect(actualGold == expectedGold)
  }

  @Test(arguments: testCases)
  func write(name: String) throws {
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
    let (arrowSchema, recordBatchesExpected) = try arrowReader.read()

    let tempDir = FileManager.default.temporaryDirectory
    let tempFile = tempDir.appendingPathComponent(UUID().uuidString + ".arrow")

    var arrowWriter = ArrowWriter(url: tempFile)
    try arrowWriter.write(
      schema: arrowSchema,
      recordBatches: recordBatchesExpected
    )
    try arrowWriter.finish()
    //    try FileManager.default.copyItem(at: tempFile, to: URL(fileURLWithPath: "/tmp/\(name).arrow"))

    let testReader = try ArrowReader(url: tempFile)
    let (arrowSchemaRead, recordBatchesRead) = try testReader.read()

    for recordBatch in recordBatchesRead {
      let lengths = recordBatch.arrays.map(\.length)
      guard let first = lengths.first else {
        Issue.record("Empty batch")
        return
      }
      guard lengths.allSatisfy({ $0 == first }) else {
        Issue.record("Mixed-length batch.")
        return
      }
    }

    let actualSchema = encode(schema: arrowSchemaRead)
    let expectedSchema = testCase.schema
    let expectedBatches = testCase.batches.map { batch in
      ArrowGold.Batch(
        count: batch.count,
        columns: batch.columns.map { $0.withoutJunkData() }
      )
    }
    let expectedDictionaries = testCase.dictionaries
    let expectedGold = ArrowGold(
      schema: expectedSchema,
      batches: expectedBatches,
      dictionaries: expectedDictionaries
    )
    if actualSchema != expectedSchema {
      try diffEncodable(actualSchema, expectedSchema)
      try printCodable(actualSchema)
      try printCodable(expectedSchema)
      //      return
    }
    #expect(actualSchema == expectedSchema)
    #expect(recordBatchesRead.count == expectedBatches.count)
    let actualBatches = try encode(
      batches: recordBatchesRead, schema: arrowSchema)

    if actualBatches != expectedBatches {
      for (a, e) in zip(actualBatches, expectedBatches) where a != e {
        for (aField, eField) in zip(a.columns, e.columns) {
          if aField == eField {
            print("MATCH: \(aField)")
          } else {
            try printCodable(aField)
            try printCodable(eField)
            try diffEncodable(aField, eField)
            return
          }
        }
      }
    }

    #expect(actualBatches == expectedBatches)
    let actualGold = ArrowGold(
      schema: actualSchema,
      batches: actualBatches,
      dictionaries: nil
    )
    // The gold-standard comparison.
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
  try batches.map { recordBatch in
    var columns: [ArrowGold.Column] = []
    for (field, array) in zip(schema.fields, recordBatch.arrays) {
      let encoded = try encodeColumn(array: array, field: field)
      columns.append(encoded)
    }
    return .init(count: recordBatch.length, columns: columns)
  }
}

/// A utility to diff encodable objects, useful in tests encoding to JSON.
/// - Parameters:
///   - actual: The actual
///   - expected: The expected `Encodable` object.
///   - label: An optional label to differentiate multiple diffs.
/// - Throws: An error if encoding fails or string data is unrepresentable in utf8.
func diffEncodable<T: Encodable>(
  _ actual: T,
  _ expected: T,
  label: String = ""
) throws {
  let encoder = JSONEncoder()
  encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
  let actualJSON = try encoder.encode(actual)
  let expectedJSON = try encoder.encode(expected)
  guard
    let actualString = String(data: actualJSON, encoding: .utf8),
    let expectedString = String(data: expectedJSON, encoding: .utf8)
  else {
    throw ArrowError(.runtimeError("Invalid UTF-8 data."))
  }
  let actualLines = actualString.split(separator: "\n")
  let expectedLines = expectedString.split(separator: "\n")
  let maxLines = max(actualLines.count, expectedLines.count)
  var hasDifferences = false
  for i in 0..<maxLines {
    let actualLine = i < actualLines.count ? actualLines[i] : ""
    let expectedLine = i < expectedLines.count ? expectedLines[i] : ""
    if actualLine != expectedLine {
      if !hasDifferences {
        print("\n== Differences found\(label.isEmpty ? "" : " in \(label)") ==")
        hasDifferences = true
      }
      print("Line \(i + 1):")
      print("  - \(expectedLine)")
      print("  + \(actualLine)")
    }
  }
  if !hasDifferences {
    print("✓ No differences\(label.isEmpty ? "" : " in \(label)")")
  }
}
