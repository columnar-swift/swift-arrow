// Copyright 2025 The Apache Software Foundation
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

import Testing

@testable import Arrow

struct BufferTests {

  @Test func example() {

    let bufferBuilder = FixedBufferBuilder<UInt32>()
    bufferBuilder.append(1)
    bufferBuilder.append(2)
    let buffers = bufferBuilder.finish()
    for buffer in buffers {
      print(buffer.capacity)
    }

  }
}
