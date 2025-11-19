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

    let outputUrl = FileManager.default.temporaryDirectory
      .appending(path: "bool-test.arrow")
    let writer = ArrowWriter(url: outputUrl)

//    writer.write(recordBatch: recordBatch)

    //    func writeBoolData() {
    //      alloc := memory.NewGoAllocator()
    //      schema := arrow.NewSchema([]arrow.Field{
    //        {Name: "one", Type: arrow.FixedWidthTypes.Boolean},
    //        {Name: "two", Type: arrow.BinaryTypes.String},
    //      }, nil)
    //
    //      b := array.NewRecordBuilder(alloc, schema)
    //      defer b.Release()
    //
    //      b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{true, false}, nil)
    //      b.Field(0).(*array.BooleanBuilder).AppendNull()
    //      b.Field(0).(*array.BooleanBuilder).AppendValues([]bool{false, true}, nil)
    //      b.Field(1).(*array.StringBuilder).AppendValues([]string{"zero", "one", "two", "three", "four"}, nil)
    //      rec := b.NewRecord()
    //      defer rec.Release()
    //
    //      writeBytes(rec, "testdata_bool.arrow")
    //    }

    //    // read existing file
    //    let fileURL = try loadArrowResource(name: "testdata_bool")
    //    let arrowReader = ArrowReader()
    ////    let fileRBs = try checkBoolRecordBatch(arrowReader.fromFile(fileURL))
    //    let arrowWriter = ArrowWriter()
    //    // write data from file to a stream
    //    let writerInfo = ArrowWriter.Info(
    //      .recordbatch, schema: fileRBs[0].schema, batches: fileRBs)
    //    switch arrowWriter.writeFile(writerInfo) {
    //    case .success(let writeData):
    //      // read stream back into recordbatches
    //      try checkBoolRecordBatch(arrowReader.readFile(writeData))
    //    case .failure(let error):
    //      throw error
    //    }
    //    // write file record batches to another file
    //    let outputUrl = FileManager.default.temporaryDirectory
    //      .appending(path: "testfilewriter_bool.arrow")
    //    switch arrowWriter.toFile(outputUrl, info: writerInfo) {
    //    case .success:
    //      try checkBoolRecordBatch(arrowReader.fromFile(outputUrl))
    //    case .failure(let error):
    //      throw error
    //    }
  }

}
