// ArrowTestingIPC.swift
// Arrow
//
// Created by Will Temperley on 26/11/2025. All rights reserved.
// Copyright 2025 Will Temperley.
//
// Copying or reproduction of this file via any medium requires prior express
// written permission from the copyright holder.
// -----------------------------------------------------------------------------
///
/// Implementation notes, links and internal documentation go here.
///
// -----------------------------------------------------------------------------

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
///
struct ArrowTestingJSON {

  static let testCases: [String] = [
    "generated_primitive",
    "generated_primitive_no_batches",
    "generated_primitive_zerolength",
    "generated_binary",
//        "generated_binary_zerolength",
    //    "generated_binary_no_batches",
    //    "generated_custom_metadata",
    //    "generated_nested",
  ]

  //  @Test(.serialized, arguments: testCases)
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

    for (testBatch, recordBatch) in zip(testCase.batches, recordBatches) {
      for (
        (arrowField, arrowArray),
        (expectedField, expectedColumn)
      ) in zip(
        zip(arrowSchema.fields, recordBatch.arrays),
        zip(testCase.schema.fields, testBatch.columns)
      ) {
        let actual = try encodeColumn(array: arrowArray, field: arrowField)
        let expected = expectedColumn.withoutJunkData()

        #expect(actual == expected)

        // This is just useful for pin-pointing differences.
        if actual != expected {
          print(expectedColumn.name)
          #expect(actual.validity == expected.validity)
          #expect(actual.offset == expected.offset)

          if actual.data != expected.data {
            guard let actualData = actual.data,
              let expectedData = expected.data, let validity = actual.validity
            else {
              fatalError()
            }

            for (i, isValid) in validity.enumerated() {
              if isValid == 1 {
                let aV = actualData[i]
                let eV = expectedData[i]
                #expect(aV == eV)
              }
            }
          }
        }
      }
    }
  }
}
