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
import Foundation
import Testing

@testable import ArrowIPC

func printTestJSON(_ value: ArrowGold) throws {
  let encoder = JSONEncoder()
  encoder.outputFormatting = .prettyPrinted
  let result = try encoder.encode(value)
  guard let formattedString = String(data: result, encoding: .utf8) else {
    throw ArrowError.unknownError("Unable to encode JSON.")
  }
  print(formattedString)
}

func loadTestResource(
  name: String, withExtension ext: String = "arrow", subdirectory: String = ""
) throws(ArrowError) -> URL {
  if let resource = Bundle.module.url(
    forResource: name,
    withExtension: ext,
    subdirectory: "Resources/\(subdirectory)"
  ) {
    return resource
  } else {
    throw .runtimeError("Couldn't find \(name).\(ext) in the test resources.")
  }
}

func checkBoolRecordBatch(recordBatch: RecordBatch) {

  #expect(recordBatch.length == 5)
  #expect(recordBatch.arrays.count == 2)
  #expect(recordBatch.schema.fields.count == 2)
  #expect(recordBatch.schema.fields[0].name == "one")
  #expect(recordBatch.schema.fields[0].type == .boolean)
  #expect(recordBatch.schema.fields[1].name == "two")
  #expect(recordBatch.schema.fields[1].type == .utf8)

  guard let one = recordBatch.arrays[0] as? ArrowArrayBoolean
  else {
    Issue.record("Failed to cast column to ArrowBooleanArray")
    return
  }
  #expect(one[0] == true)
  #expect(one[1] == false)
  #expect(one[2] == nil)
  #expect(one[3] == false)
  #expect(one[4] == true)

  guard
    let utf8Column = recordBatch.arrays[1] as? StringArrayProtocol
  else {
    Issue.record("Failed to cast column to ArrowUtf8Array")
    return
  }

  #expect(utf8Column[0] == "zero")
  #expect(utf8Column[1] == "one")
  #expect(utf8Column[2] == "two")
  #expect(utf8Column[3] == "three")
  #expect(utf8Column[4] == "four")
}

extension Data {
  init?(hex: String) {
    let len = hex.count / 2
    var data = Data(capacity: len)
    var index = hex.startIndex
    for _ in 0..<len {
      let nextIndex = hex.index(index, offsetBy: 2)
      if let byte = UInt8(hex[index..<nextIndex], radix: 16) {
        data.append(byte)
      } else {
        return nil
      }
      index = nextIndex
    }
    self = data
  }
}

/// Pretty print an encodable value.
/// - Parameter value: The value to print.
/// - Throws: On failed utf8 encoding.
func printCodable<T: Encodable>(_ value: T) throws {
  let encoder = JSONEncoder()
  encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
  let data = try encoder.encode(value)
  guard let formattted = String(data: data, encoding: .utf8) else {
    throw ArrowError.invalid("UTF-8 encode failed.")
  }
  print(formattted)
}
