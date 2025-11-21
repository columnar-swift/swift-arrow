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

struct ArrowWriterTests {

  @Test func writeBasics() throws {

    let outputUrl = FileManager.default.temporaryDirectory
      .appending(path: "bool-test.arrow")
    let writer = ArrowWriter(url: outputUrl)
    #expect(writer.data.count == 8)

  }

  @Test func writeBoolean() throws {

    let schema: ArrowSchema = ArrowSchema.Builder()
      .addField("one", type: .boolean, isNullable: true)
      .addField("two", type: .utf8, isNullable: true)
      .finish()

    let builder = ArrayBuilderBoolean()
    builder.append(true)
    builder.append(false)
    builder.appendNull()
    builder.append(false)
    builder.append(true)
    let one = builder.finish()

    let builder2 = ArrayBuilderString()
    builder2.append("zero")
    builder2.append("one")
    builder2.append("two")
    builder2.append("three")
    builder2.append("four")
    let two = builder2.finish()

    let recordBatch = RecordBatch(schema: schema, columns: [one, two])

    checkBoolRecordBatch(recordBatch: recordBatch)

    let outputUrl = FileManager.default.temporaryDirectory
      .appending(path: "bool-test.arrow")
    var writer = ArrowWriter(url: outputUrl)
    try writer.write(schema: schema, recordBatches: [recordBatch])
    try writer.finish()

    let arrowReader = try ArrowReader(url: outputUrl)
    let (arrowSchema, recordBatches) = try arrowReader.read()

    for recordBatch in recordBatches {
      checkBoolRecordBatch(recordBatch: recordBatch)
    }
    //    try FileManager.default.copyItem(at: outputUrl, to: URL(fileURLWithPath: "/tmp/bool-test-swift.arrow"))

  }

}
