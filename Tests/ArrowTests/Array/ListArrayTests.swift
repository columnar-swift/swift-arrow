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

struct ListArrayTests {
  
  @Test func example() {
    
    let builder = ArrayBuilderList(valueBuilder: ArrayBuilderFixedWidth<Int32>())

    builder.append { childBuilder in
      childBuilder.append(1)
      childBuilder.append(2)
    }

    builder.appendNull()

    builder.append { childBuilder in
      childBuilder.append(3)
      childBuilder.append(4)
      childBuilder.append(5)
    }

    let listArray = builder.finish()  // [[1,2], null, [3,4,5]]
    
    let list0 = listArray[0]
    let list1 = listArray[2]
    #expect(list0?.length == 2)
    #expect(list0?[0] == 1)
    #expect(list0?[1] == 2)
    
    #expect(listArray[1] == nil)
    
    #expect(list1?.length == 3)
    #expect(list1?[0] == 3)
    #expect(list1?[1] == 4)
    #expect(list1?[2] == 5)
  }
  
}
