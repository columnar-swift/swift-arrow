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

func loadArrowResource(name: String) throws(ArrowError) -> URL {
  if let resource = Bundle.module.url(
    forResource: name,
    withExtension: "arrow",
    subdirectory: "Resources"
  ) {
    return resource
  } else {
    throw .runtimeError("Couldn't find \(name).arrow in the test resources.")
  }
}

func checkBoolRecordBatch(recordBatch: RecordBatch) {

  #expect(recordBatch.length == 5)
  #expect(recordBatch.columns.count == 2)
  #expect(recordBatch.schema.fields.count == 2)
  #expect(recordBatch.schema.fields[0].name == "one")
  #expect(recordBatch.schema.fields[0].type == .boolean)
  #expect(recordBatch.schema.fields[1].name == "two")
  #expect(recordBatch.schema.fields[1].type == .utf8)

  guard let one = recordBatch.columns[0] as? ArrowArrayBoolean
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
    let utf8Column = recordBatch.columns[1] as? ArrowArrayOfString
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
