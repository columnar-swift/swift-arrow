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

struct StructArrayTests {
  
  @Test func testStructArray() {
    // Create builders for struct fields
    let idBuilder = ArrayBuilderFixedWidth<Int32>()
    let nameBuilder = ArrayBuilderVariable<String>()
    
    // Create struct builder
    let structBuilder = ArrayBuilderStruct(fields: [
      ("id", idBuilder),
      ("name", nameBuilder)
    ])
    
    // Append some structs
    structBuilder.append(["id": Int32(1), "name": "Alice"])
    structBuilder.append(["id": Int32(2), "name": "Bob"])
    structBuilder.appendNull()
    structBuilder.append(["id": Int32(3), "name": "Charlie"])
    
    // Finish building
    let structArray = structBuilder.finish()
    
    // Verify results
    #expect(structArray.length == 4)
    
    let row0 = structArray[0]!
    #expect(row0["id"] as! Int32 == 1)
    #expect(row0["name"] as! String == "Alice")
    
    let row1 = structArray[1]!
    #expect(row1["id"] as! Int32 == 2)
    #expect(row1["name"] as! String == "Bob")
    
    #expect(structArray[2] == nil)  // null struct
    
    let row3 = structArray[3]!
    #expect(row3["id"] as! Int32 == 3)
    #expect(row3["name"] as! String == "Charlie")
    
  }
  
}
