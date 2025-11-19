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

struct ReadmeExamples {

  @Test func int8Array() throws {
    let swiftArray: [Int8?] = [1, nil, 2, 3, nil, 4]
    let arrayBuilder: ArrayBuilderFixedWidth<Int8> = .init()
    for value in swiftArray {
      if let value {
        arrayBuilder.append(value)
      } else {
        arrayBuilder.appendNull()
      }
    }
    let arrowArray = arrayBuilder.finish()
    for i in 0..<swiftArray.count {
      #expect(arrowArray[i] == swiftArray[i])
    }
    //    for i in 0..<expected.count {
    //      print("Buffer val: \(array.valueBuffer[i])")
    //    }
    //
    //    let bitPacked = array.nullBuffer as! BitPackedNullBuffer
    //    print(bitPacked.buffer[0])
  }

  @Test func stringArray() throws {
    let swiftArray: [String?] = ["ab", nil, "c", "", "."]
    let arrayBuilder: ArrayBuilderVariableLength<String> = .init()
    for value in swiftArray {
      if let value {
        arrayBuilder.append(value)
      } else {
        arrayBuilder.appendNull()
      }
    }
    let arrowArray = arrayBuilder.finish()
    #expect(arrowArray[0] == "ab")
    #expect(arrowArray[1] == nil)
    #expect(arrowArray[2] == "c")
    #expect(arrowArray[3] == "")
    #expect(arrowArray[4] == ".")

    //    for i in 0..<arrowArray.offsetsBuffer.length {
    //      print("offsets[i]: \(arrowArray.offsetsBuffer[i])")
    //    }
    //
    //    var x: [UInt8] = []
    //    for i in 0..<4 {
    //      x.append(arrowArray.valueBuffer.buffer[i])
    //    }
    //
    //    let values: [UInt8] = [97, 98, 99, 46]
    //    print(values[0..<2])  // [97, 98]
    //    print(values[2..<2])  // []
    //    print(values[2..<3])  // [99]
    //    print(values[3..<4])  // [46]

    //    print(String(data: Data(values[0..<2]), encoding: .utf8))

  }
}
